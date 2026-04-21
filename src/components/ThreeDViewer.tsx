import React, { useEffect, useMemo, useRef, useState } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { OBJLoader } from 'three/examples/jsm/loaders/OBJLoader.js';

type Props = {
  fileUrl: string;
  className?: string;
  onLoaded?: () => void;
  onError?: (err: Error) => void;
};

function fitCameraToObject(camera: THREE.PerspectiveCamera, object: THREE.Object3D, controls?: OrbitControls) {
  const box = new THREE.Box3().setFromObject(object);
  const size = box.getSize(new THREE.Vector3());
  const center = box.getCenter(new THREE.Vector3());

  const maxDim = Math.max(size.x, size.y, size.z);
  const fov = (camera.fov * Math.PI) / 180;
  let cameraZ = Math.abs((maxDim / 2) / Math.tan(fov / 2));
  cameraZ *= 1.6;

  camera.position.set(center.x, center.y + maxDim * 0.15, center.z + cameraZ);
  camera.near = Math.max(0.01, cameraZ / 200);
  camera.far = cameraZ * 500;
  camera.updateProjectionMatrix();

  if (controls) {
    controls.target.copy(center);
    controls.update();
  }
}

export default function ThreeDViewer({ fileUrl, className, onLoaded, onError }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const rendererRef = useRef<THREE.WebGLRenderer | null>(null);
  const rafRef = useRef<number | null>(null);

  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const urlKey = useMemo(() => String(fileUrl || '').trim(), [fileUrl]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    if (!urlKey) return;

    setReady(false);
    setError(null);

    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x0b1220);

    const camera = new THREE.PerspectiveCamera(45, 1, 0.01, 1000);
    camera.position.set(0, 0.5, 3);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    renderer.outputColorSpace = THREE.SRGBColorSpace;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.0;
    renderer.shadowMap.enabled = false;

    rendererRef.current = renderer;
    container.appendChild(renderer.domElement);

    const ambient = new THREE.AmbientLight(0xffffff, 0.55);
    scene.add(ambient);

    const hemi = new THREE.HemisphereLight(0xffffff, 0x1f2937, 0.75);
    hemi.position.set(0, 1, 0);
    scene.add(hemi);

    const dir = new THREE.DirectionalLight(0xffffff, 0.7);
    dir.position.set(2, 3, 2);
    scene.add(dir);

    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.08;
    controls.screenSpacePanning = true;
    controls.minDistance = 0.1;
    controls.maxDistance = 50;

    const resize = () => {
      const w = container.clientWidth || 1;
      const h = container.clientHeight || 1;
      camera.aspect = w / h;
      camera.updateProjectionMatrix();
      renderer.setSize(w, h, false);
    };

    const ro = new ResizeObserver(() => resize());
    ro.observe(container);
    resize();

    let disposed = false;
    const loader = new OBJLoader();

    const material = new THREE.MeshStandardMaterial({
      color: new THREE.Color('#e5e7eb'),
      roughness: 0.55,
      metalness: 0.05,
    });

    const applyMaterial = (obj: THREE.Object3D) => {
      obj.traverse((child) => {
        const mesh = child as THREE.Mesh;
        if ((mesh as any).isMesh) {
          mesh.material = material;
          mesh.castShadow = false;
          mesh.receiveShadow = false;
        }
      });
    };

    loader.load(
      urlKey,
      (obj) => {
        if (disposed) return;
        applyMaterial(obj);
        scene.add(obj);
        fitCameraToObject(camera, obj, controls);
        setReady(true);
        onLoaded?.();
      },
      undefined,
      (e) => {
        if (disposed) return;
        const err = new Error(e?.message || 'OBJ 加载失败');
        setError(err.message);
        onError?.(err);
      }
    );

    const tick = () => {
      controls.update();
      renderer.render(scene, camera);
      rafRef.current = window.requestAnimationFrame(tick);
    };
    tick();

    return () => {
      disposed = true;
      ro.disconnect();
      if (rafRef.current != null) window.cancelAnimationFrame(rafRef.current);
      controls.dispose();

      scene.traverse((o) => {
        const m = o as THREE.Mesh;
        if ((m as any).isMesh) {
          const geom = (m as any).geometry as THREE.BufferGeometry | undefined;
          if (geom && typeof geom.dispose === 'function') geom.dispose();
        }
      });
      material.dispose();

      try {
        container.removeChild(renderer.domElement);
      } catch {
        // ignore
      }
      renderer.dispose();
      rendererRef.current = null;
    };
  }, [urlKey, onLoaded, onError]);

  return (
    <div className={className} ref={containerRef}>
      {!ready && !error ? (
        <div className="absolute inset-0 pointer-events-none" />
      ) : null}
      {error ? (
        <div className="absolute inset-0 flex items-center justify-center text-sm text-red-300 bg-black/20">
          {error}
        </div>
      ) : null}
    </div>
  );
}

