import React, { useEffect, useMemo, useRef, useState } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { OBJLoader } from 'three/examples/jsm/loaders/OBJLoader.js';
import { GLTFLoader } from 'three/examples/jsm/loaders/GLTFLoader.js';
import { STLLoader } from 'three/examples/jsm/loaders/STLLoader.js';
import { getStorageBaseUrl } from '@/lib/storageBaseUrl';
import { extractLast3DMetrics, type Last3DMetrics } from '@/lib/last3dMetrics';
import { audienceLabelZh, getTargetLengthMm, parseAudience, type ShoeAudience } from '@/lib/shoeStandards';

type Props = {
  fileUrl: string;
  glbUrl?: string | null;
  objUrl?: string | null;
  assetStatus?: 'ready' | 'processing';
  /** 与清单行 `target_audience` 对齐；未传时按 MEN(275mm) 处理。仅对 legacy OBJ 路径生效。 */
  targetAudience?: string;
  /**
   * 来自 GET /api/asset-meta 的预处理测绘；与 .glb 同用时跳过后端已算过的 L/W/H 与前端重复几何扫描。
   */
  precomputedMetrics?: Last3DMetrics | null;
  /** 变更时与 key 同用，强制重挂 viewer（如 meta `updatedAt`） */
  precomputedKey?: string;
  /** 每次变化会刷新 .glb 的 cache buster，例如资源更新后重开弹窗 */
  glbCacheToken?: string | number;
  className?: string;
  onLoaded?: () => void;
  onError?: (err: Error) => void;
  onMetrics?: (m: Last3DMetrics) => void;
};

const GLB_PREVIEW_TIMEOUT_MS = 2000;

function resolveAssetUrl(fileUrl: string) {
  const u = String(fileUrl || '').trim();
  if (!u) return '';
  if (u.startsWith('http://') || u.startsWith('https://')) return u;
  if (u.startsWith('/')) return `${getStorageBaseUrl()}${u}`;
  return `${getStorageBaseUrl()}/${u.replace(/^\//, '')}`;
}

/** 与后端 asset-details 返回的 /storage/... 路径配合，穿透 CDN/浏览器缓存 */
function withCachePierce(url: string, token: string) {
  const u = String(url || '').trim();
  if (!u) return u;
  const [beforeHash, ...restHash] = u.split('#');
  const frag = restHash.length ? restHash.join('#') : undefined;
  const sep = beforeHash.includes('?') ? '&' : '?';
  const out = `${beforeHash}${sep}t=${encodeURIComponent(token)}`;
  return frag !== undefined ? `${out}#${frag}` : out;
}

function centerObject(obj: THREE.Object3D) {
  const box = new THREE.Box3().setFromObject(obj);
  const c = box.getCenter(new THREE.Vector3());
  obj.position.sub(c);
  obj.updateMatrixWorld(true);
}

function maxAxisLength(obj: THREE.Object3D) {
  obj.updateMatrixWorld(true);
  const s = new THREE.Box3().setFromObject(obj).getSize(new THREE.Vector3());
  return Math.max(s.x, s.y, s.z, 0);
}

function applyIndustryMmNormalization(
  obj: THREE.Object3D,
  audience: ShoeAudience
): { rawMax: number; lenBeforeTargetMm: number; scaleToTarget: number; targetMm: number; audience: ShoeAudience } {
  const targetMm = getTargetLengthMm(audience);
  const raw0 = maxAxisLength(obj);
  if (raw0 < 0.5 && raw0 > 0.05) {
    obj.scale.multiplyScalar(1000);
  }
  obj.updateMatrixWorld(true);
  let m = maxAxisLength(obj);
  if (m > 1500 && m < 5000) {
    obj.scale.multiplyScalar(0.1);
  }
  obj.updateMatrixWorld(true);
  const lenBeforeTargetMm = maxAxisLength(obj);
  const scaleToTarget = lenBeforeTargetMm < 1e-9 ? 1 : targetMm / lenBeforeTargetMm;
  obj.scale.multiplyScalar(scaleToTarget);
  obj.updateMatrixWorld(true);
  centerObject(obj);
  return { rawMax: raw0, lenBeforeTargetMm, scaleToTarget, targetMm, audience };
}

function isGeometryEmpty(obj: THREE.Object3D) {
  let n = 0;
  obj.traverse((ch) => {
    if ((ch as any).isMesh) {
      const p = (ch as THREE.Mesh).geometry?.getAttribute('position');
      if (p) n += p.count;
    }
  });
  if (n === 0) return true;
  obj.updateMatrixWorld(true);
  const s = new THREE.Box3().setFromObject(obj).getSize(new THREE.Vector3());
  return Math.max(s.x, s.y, s.z) < 1e-12;
}

type ProcessOpts = {
  isGlb: boolean;
  precomputed: Last3DMetrics | null | undefined;
  targetAudience: string | undefined;
  urlKey: string;
  scene: THREE.Scene;
  camera: THREE.PerspectiveCamera;
  controls: OrbitControls;
  container: HTMLDivElement;
  resize: () => void;
  phongMat: THREE.MeshPhongMaterial;
  setError: (s: string | null) => void;
  onError: (e: Error) => void;
  onMetrics: (m: Last3DMetrics) => void;
  debugState: { boxHelper: THREE.BoxHelper | null };
};

function applyPhongShading(obj: THREE.Object3D, mat: THREE.MeshPhongMaterial) {
  obj.traverse((ch) => {
    const o = ch as THREE.Mesh;
    if ((o as any).isMesh) {
      const old = o.material;
      if (Array.isArray(old)) old.forEach((m) => (m as THREE.Material).dispose?.());
      else (old as THREE.Material | undefined)?.dispose?.();
      o.material = mat;
    }
  });
}

function processLoadedObject(obj: THREE.Object3D, o: ProcessOpts) {
  const { isGlb, precomputed, targetAudience, camera, controls, phongMat, setError, onError, resize, container, onMetrics, debugState, scene, urlKey } = o;

  const meshes: THREE.Mesh[] = [];
  obj.traverse((ch) => {
    if ((ch as any).isMesh) meshes.push(ch as THREE.Mesh);
  });
  for (const mesh of meshes) {
    const g = (mesh as any).geometry as THREE.BufferGeometry | undefined;
    if (g?.isBufferGeometry) {
      try {
        g.computeVertexNormals();
      } catch {
        // ignore
      }
    }
  }

  centerObject(obj);

  if (isGeometryEmpty(obj)) {
    const msg = '错误：模型几何体数据为空或尺寸异常';
    setError(msg);
    onError(new Error(msg));
    return false;
  }

  const usePre = Boolean(precomputed) && isGlb;
  if (!isGlb) {
    const aud = parseAudience(targetAudience);
    const sync = applyIndustryMmNormalization(obj, aud);
    // eslint-disable-next-line no-console
    console.log(
      `[StandardSync] 类别: ${audienceLabelZh(sync.audience)} | 原始长度: ${sync.rawMax} | 缩放系数: ${sync.scaleToTarget.toFixed(4)} | 最终长度: ${sync.targetMm}mm`
    );
  }

  let metrics: Last3DMetrics;
  if (usePre && precomputed) {
    metrics = precomputed;
  } else {
    metrics = extractLast3DMetrics(obj);
  }

  applyPhongShading(obj, phongMat);

  const box = new THREE.Box3().setFromObject(obj);
  const size = box.getSize(new THREE.Vector3());
  const target = new THREE.Vector3();
  box.getCenter(target);
  controls.target.copy(target);

  let d: number;
  if (!isGlb) {
    const aud2 = parseAudience(targetAudience);
    d = getTargetLengthMm(aud2) * 1.5;
  } else {
    d = Math.max(size.x, size.y, size.z, 1) * 1.5;
  }
  const off = new THREE.Vector3(0.7, 0.45, 0.8).normalize().multiplyScalar(d);
  camera.position.copy(target.clone().add(off));
  camera.near = Math.max(0.1, d / 2000);
  camera.far = d * 80;
  camera.lookAt(target);
  camera.updateProjectionMatrix();
  controls.update();

  scene.add(obj);
  debugState.boxHelper = new THREE.BoxHelper(obj, 0xff8800);
  scene.add(debugState.boxHelper);
  onMetrics(metrics);

  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      resize();
      const w = container.clientWidth;
      const h = container.clientHeight;
      if (w > 0 && h > 0) {
        camera.aspect = w / h;
        camera.updateProjectionMatrix();
      }
    });
  });

  void urlKey;
  return true;
}

function loadTextWithXhr(
  url: string,
  onProgress: (pct: number | null) => void,
  signal: { aborted: boolean }
): Promise<string> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('GET', url, true);
    xhr.responseType = 'text';
    xhr.onprogress = (e) => {
      if (signal.aborted) return;
      if (e.lengthComputable && e.total > 0) {
        onProgress(Math.max(0, Math.min(100, Math.round((e.loaded / e.total) * 100))));
      } else {
        onProgress(null);
      }
    };
    xhr.onload = () => {
      if (signal.aborted) return;
      onProgress(100);
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(xhr.responseText as string);
        return;
      }
      reject(new Error(`HTTP ${xhr.status} ${String(xhr.statusText || '').trim()}`));
    };
    xhr.onerror = () => {
      if (signal.aborted) return;
      reject(new Error('网络错误或无法访问资源（CORS/代理）'));
    };
    xhr.send();
  });
}

function loadArrayBufferWithXhr(
  url: string,
  onProgress: (pct: number | null) => void,
  signal: { aborted: boolean }
): Promise<ArrayBuffer> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('GET', url, true);
    xhr.responseType = 'arraybuffer';
    xhr.onprogress = (e) => {
      if (signal.aborted) return;
      if (e.lengthComputable && e.total > 0) {
        onProgress(Math.max(0, Math.min(100, Math.round((e.loaded / e.total) * 100))));
      } else {
        onProgress(null);
      }
    };
    xhr.onload = () => {
      if (signal.aborted) return;
      onProgress(100);
      if (xhr.status >= 200 && xhr.status < 300 && xhr.response) {
        resolve(xhr.response as ArrayBuffer);
        return;
      }
      reject(new Error(`HTTP ${xhr.status} ${String(xhr.statusText || '').trim()}`));
    };
    xhr.onerror = () => {
      if (signal.aborted) return;
      reject(new Error('网络错误或无法访问资源（CORS/代理）'));
    };
    xhr.send();
  });
}

async function headValidateAssetUrl(resolvedUrl: string) {
  const resp = await fetch(resolvedUrl, { method: 'HEAD' });
  const ct = String(resp.headers.get('content-type') || '').toLowerCase();
  const isHtml = ct.includes('text/html');
  return { ok: resp.ok, status: resp.status, contentType: ct, isHtml };
}

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = window.setTimeout(() => reject(new Error(`${label} timeout after ${ms}ms`)), ms);
    promise.then(
      (value) => {
        window.clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        window.clearTimeout(timer);
        reject(error);
      }
    );
  });
}

function deriveObjUrlFromGlbUrl(glbResolvedUrl: string) {
  try {
    const u = new URL(glbResolvedUrl, window.location.origin);
    u.pathname = u.pathname.replace(/\.glb$/i, '.obj');
    // 清理 query：保留原 query，但 cache buster 会在外层加
    return u.toString();
  } catch {
    return glbResolvedUrl.replace(/\.glb(\?.*)?$/i, '.obj');
  }
}

function resourcePathForUrl(resolved: string) {
  try {
    const u = new URL(resolved, typeof window !== 'undefined' ? window.location.origin : 'http://local');
    const s = u.pathname;
    return s.replace(/\/[^/]+$/, '/');
  } catch {
    return '';
  }
}

export default function ThreeDViewer({
  fileUrl,
  glbUrl,
  objUrl,
  assetStatus,
  targetAudience,
  precomputedMetrics,
  precomputedKey,
  glbCacheToken,
  className,
  onLoaded,
  onError,
  onMetrics,
}: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const onMetricsRef = useRef(onMetrics);
  const onLoadedRef = useRef(onLoaded);
  const onErrorRef = useRef(onError);
  onMetricsRef.current = onMetrics;
  onLoadedRef.current = onLoaded;
  onErrorRef.current = onError;

  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<number | null>(null);

  const urlKey = useMemo(() => resolveAssetUrl(String(fileUrl || '').trim()), [fileUrl]);
  const glbKey = useMemo(() => resolveAssetUrl(String(glbUrl || '').trim()), [glbUrl]);
  const objKey = useMemo(() => resolveAssetUrl(String(objUrl || '').trim()), [objUrl]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    if (!urlKey) return;

    const signal = { aborted: false };

    setReady(false);
    setError(null);
    setProgress(0);

    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0xeeeeee);

    const camera = new THREE.PerspectiveCamera(50, 1, 0.001, 1_000_000);
    camera.position.set(8, 8, 8);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false, powerPreference: 'high-performance' });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    renderer.outputColorSpace = THREE.SRGBColorSpace;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.4;
    renderer.shadowMap.enabled = false;
    renderer.domElement.style.width = '100%';
    renderer.domElement.style.height = '100%';
    renderer.domElement.style.display = 'block';
    container.appendChild(renderer.domElement);

    const ambient = new THREE.AmbientLight(0xffffff, 0.75);
    scene.add(ambient);
    const hemi = new THREE.HemisphereLight(0xffffff, 0xe5e5e5, 0.45);
    hemi.position.set(0, 1, 0);
    scene.add(hemi);
    const dir = new THREE.DirectionalLight(0xffffff, 0.9);
    dir.position.set(400, 800, 500);
    scene.add(dir);
    const fill = new THREE.DirectionalLight(0xffffff, 0.4);
    fill.position.set(-300, 200, -200);
    scene.add(fill);

    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.06;
    controls.screenSpacePanning = true;
    controls.minDistance = 0.05;
    controls.maxDistance = 5e3;

    const phongMat = new THREE.MeshPhongMaterial({
      color: 0x888888,
      flatShading: true,
      side: THREE.DoubleSide,
      shininess: 24,
    });

    const debugState: { boxHelper: THREE.BoxHelper | null } = { boxHelper: null };

    const grid = new THREE.GridHelper(200, 20, 0xbbbbbb, 0xcccccc);
    grid.position.y = 0;
    scene.add(grid);
    const axes = new THREE.AxesHelper(150);
    scene.add(axes);

    const resize = () => {
      const w = container.clientWidth || 1;
      const h = container.clientHeight || 1;
      camera.aspect = w / h;
      camera.updateProjectionMatrix();
      renderer.setSize(w, h, false);
    };

    const ro = new ResizeObserver(() => {
      resize();
    });
    ro.observe(container);
    resize();

    const loaderObj = new OBJLoader();
    const loaderGltf = new GLTFLoader();
    const loaderStl = new STLLoader();

    const disposeGeometries = (o: THREE.Object3D) => {
      o.traverse((n) => {
        const m = n as { geometry?: THREE.BufferGeometry | null } & { material?: unknown };
        if (m.geometry) m.geometry.dispose();
      });
    };

    const preferredGlbUrl = glbKey || (urlKey.toLowerCase().endsWith('.glb') ? urlKey : '');
    const preferredObjUrl = objKey || (preferredGlbUrl ? deriveObjUrlFromGlbUrl(preferredGlbUrl) : urlKey);
    const isGlb = Boolean(preferredGlbUrl);
    const pierceToken =
      typeof glbCacheToken === 'number' || typeof glbCacheToken === 'string' ? String(glbCacheToken) : String(Date.now());
    const glbFetchUrl = preferredGlbUrl ? withCachePierce(preferredGlbUrl, pierceToken) : '';
    const objFetchUrl = preferredObjUrl ? withCachePierce(preferredObjUrl, pierceToken) : '';
    const urlFetchUrl = urlKey ? withCachePierce(urlKey, pierceToken) : '';

    void (async () => {
      try {
        const procBase: Omit<ProcessOpts, 'onMetrics' | 'debugState'> = {
          isGlb,
          precomputed: precomputedMetrics,
          targetAudience,
          urlKey,
          scene,
          camera,
          controls,
          container,
          resize,
          phongMat,
          setError,
          onError: (e) => onErrorRef.current?.(e),
        };

        let headGlbStatusForMessage = 0;

        const loadObjOrStlSource = async (sourceUrl: string, isFallback: boolean) => {
          const headSource = await headValidateAssetUrl(sourceUrl);
          if (!headSource.ok || headSource.isHtml) {
            const msg = isFallback
              ? `文件路径错误 (GLB:${headGlbStatusForMessage} / OBJ:${headSource.status})`
              : `文件路径错误 (HTTP ${headSource.status})`;
            setError(msg);
            onErrorRef.current?.(new Error(msg));
            setProgress(null);
            return null;
          }
          if (sourceUrl.toLowerCase().endsWith('.stl')) {
            const ab = await loadArrayBufferWithXhr(
              sourceUrl,
              (p) => {
                if (!signal.aborted) setProgress(p);
              },
              signal
            );
            if (signal.aborted) return null;
            const geometry = loaderStl.parse(ab);
            return new THREE.Mesh(geometry, phongMat.clone());
          }
          const text = await loadTextWithXhr(
            sourceUrl,
            (p) => {
              if (!signal.aborted) setProgress(p);
            },
            signal
          );
          if (signal.aborted) return null;
          return loaderObj.parse(text);
        };

        if (isGlb) {
          // 1) 优先 GLB（快）。若 GLB 404 / 加载失败，则自动降级 OBJ
          let headGlb: Awaited<ReturnType<typeof headValidateAssetUrl>> = { ok: false, status: 0, contentType: '', isHtml: false };
          try {
            headGlb = await withTimeout(headValidateAssetUrl(glbFetchUrl), GLB_PREVIEW_TIMEOUT_MS, 'GLB HEAD');
            headGlbStatusForMessage = headGlb.status;
          } catch (e) {
            headGlbStatusForMessage = 0;
            // eslint-disable-next-line no-console
            console.warn('[ThreeDViewer] GLB HEAD 超时/失败，立即降级 OBJ:', e);
          }
          if (!headGlb.ok && assetStatus === 'ready' && objFetchUrl) {
            // eslint-disable-next-line no-console
            console.warn(`[ThreeDViewer] 后端状态 ready 但 GLB 不可访问(HTTP ${headGlb.status})，立即加载源文件降级。`);
          }
          if (headGlb.ok && !headGlb.isHtml) {
            try {
              const ab = await withTimeout(
                loadArrayBufferWithXhr(
                  glbFetchUrl,
                  (p) => {
                    if (!signal.aborted) setProgress(p);
                  },
                  signal
                ),
                GLB_PREVIEW_TIMEOUT_MS,
                'GLB load'
              );
              if (signal.aborted) return;
              const basePath = resourcePathForUrl(preferredGlbUrl);
              const gltf = await new Promise<{ scene: THREE.Object3D }>((resolve, reject) => {
                loaderGltf.parse(
                  ab,
                  basePath,
                  (g) => resolve(g as { scene: THREE.Object3D }),
                  (err) => reject(err ?? new Error('GLB parse error'))
                );
              });
              if (signal.aborted) return;
              const ok = processLoadedObject(gltf.scene, {
                ...procBase,
                onMetrics: (m) => onMetricsRef.current?.(m),
                debugState,
              });
              if (!ok) {
                setProgress(null);
                return;
              }
              setReady(true);
              onLoadedRef.current?.();
              setProgress(null);
              return;
            } catch (e) {
              // eslint-disable-next-line no-console
              console.warn('[ThreeDViewer] GLB 加载失败，降级 OBJ:', e);
            }
          }

          // 2) OBJ 降级
          const obj = await loadObjOrStlSource(objFetchUrl, true);
          if (!obj || signal.aborted) return;
          const ok = processLoadedObject(obj, {
            ...procBase,
            isGlb: false,
            precomputed: null,
            onMetrics: (m) => onMetricsRef.current?.(m),
            debugState,
          });
          if (!ok) {
            setProgress(null);
            return;
          }
        } else {
          // OBJ/STL 主路径
          const sourceUrl = objFetchUrl || urlFetchUrl;
          const obj = await loadObjOrStlSource(sourceUrl, false);
          if (!obj || signal.aborted) return;
          const ok = processLoadedObject(obj, {
            ...procBase,
            onMetrics: (m) => onMetricsRef.current?.(m),
            debugState,
          });
          if (!ok) {
            setProgress(null);
            return;
          }
        }
        setReady(true);
        onLoadedRef.current?.();
        setProgress(null);
      } catch (e) {
        if (signal.aborted) return;
        const err = e instanceof Error ? e : new Error('加载失败');
        setError(err.message);
        onErrorRef.current?.(err);
        setProgress(null);
      }
    })();

    let rafId = 0;
    const tick = () => {
      rafId = window.requestAnimationFrame(tick);
      if (signal.aborted) return;
      if (debugState.boxHelper) debugState.boxHelper.update();
      controls.update();
      renderer.render(scene, camera);
    };
    rafId = window.requestAnimationFrame(tick);

    return () => {
      signal.aborted = true;
      window.cancelAnimationFrame(rafId);
      ro.disconnect();
      if (debugState.boxHelper) {
        try {
          scene.remove(debugState.boxHelper);
          debugState.boxHelper.traverse((n) => {
            const m = n as { geometry?: THREE.BufferGeometry; material?: unknown };
            if (m.geometry) m.geometry.dispose();
            if (m.material) {
              if (Array.isArray(m.material)) m.material.forEach((x) => (x as THREE.Material).dispose());
              else (m.material as THREE.Material).dispose();
            }
          });
        } catch {
          // ignore
        }
        debugState.boxHelper = null;
      }
      disposeGeometries(scene);
      scene.clear();
      phongMat.dispose();
      controls.dispose();
      try {
        container.removeChild(renderer.domElement);
      } catch {
        // ignore
      }
      renderer.dispose();
    };
  }, [urlKey, glbKey, objKey, assetStatus, targetAudience, precomputedMetrics, precomputedKey, glbCacheToken]);

  return (
    <div className={className} ref={containerRef} style={{ position: 'relative', minHeight: 120, width: '100%', height: '100%' }}>
      {!ready && !error ? (
        <div className="absolute inset-0 pointer-events-none z-[1]">
          {progress != null && progress < 100 ? (
            <div className="absolute left-4 right-4 bottom-4 text-xs text-slate-600/90">
              <div className="flex items-center justify-between mb-1">
                <span>模型拉取中</span>
                <span>{typeof progress === 'number' ? `${progress}%` : '—'}</span>
              </div>
              {typeof progress === 'number' ? (
                <div className="h-1.5 bg-slate-200 rounded">
                  <div className="h-1.5 bg-blue-500 rounded" style={{ width: `${progress}%` }} />
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}
      {error ? (
        <div
          className="absolute inset-0 z-20 flex flex-col items-center justify-center px-4 text-center bg-black/75"
          data-three-error-overlay="1"
        >
          <p className="text-sm font-mono text-red-400 break-all max-w-full">3D 加载失败</p>
          <p className="text-lg font-mono text-red-300 font-bold mt-2 break-all max-w-full">{error}</p>
          <p className="text-xs text-slate-400 mt-2">请检查 URL、HTTP 状态与跨域/代理 (Vite → :3001)</p>
        </div>
      ) : null}
    </div>
  );
}