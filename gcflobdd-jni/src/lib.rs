//! JNI bindings exposing the GCFLOBDD packet-set engine to Java, mirroring the
//! subset of the `jdd.bdd.BDD` int-handle interface that APKeep uses.
//!
//! The Java side is a class
//! `application.wan.bdd.verifier.common.GcflobddEngine` whose native methods are
//! all `static` and take the opaque engine pointer (`long`) as their first
//! argument. See `engine.rs` for the handle-registry / refcount semantics.
//!
//! Thread confinement: the engine uses `Rc`/`RefCell` and is **not** `Send`.
//! APKeep drives a single static engine from one thread, so handles never cross
//! threads. Do not call these methods on an engine from more than one thread.

mod engine;

use std::panic::{AssertUnwindSafe, catch_unwind};

use jni::JNIEnv;
use jni::objects::{JClass, JIntArray};
use jni::sys::{jboolean, jdouble, jint, jlong};

use engine::{Engine, GrammarConfig};

/// Reconstruct a `&mut Engine` from the opaque pointer handed to Java.
///
/// # Safety
/// `ptr` must be a value previously returned by `nativeNew` and not yet passed
/// to `nativeDestroy`, and must not be used from another thread concurrently.
unsafe fn engine_mut<'a>(ptr: jlong) -> &'a mut Engine {
    unsafe { &mut *(ptr as *mut Engine) }
}

/// Run `f`, catching any Rust panic (unwinding across the FFI boundary is UB).
/// On panic, raise a Java `RuntimeException` and return `default`.
fn guard<R>(env: &mut JNIEnv, default: R, f: impl FnOnce() -> R) -> R {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                "panic in gcflobdd-jni native engine",
            );
            default
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeNew(
    mut env: JNIEnv,
    _class: JClass,
    config: jint,
) -> jlong {
    guard(&mut env, 0, || {
        let Some(cfg) = GrammarConfig::from_i32(config) else {
            return 0;
        };
        let engine = Box::new(Engine::new(cfg));
        Box::into_raw(engine) as jlong
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeDestroy(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    guard(&mut env, (), || {
        if ptr != 0 {
            // Reclaim the Box; the intentionally-leaked `Grammar` is not freed.
            drop(unsafe { Box::from_raw(ptr as *mut Engine) });
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeCreateVar(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    guard(&mut env, 0, || unsafe { engine_mut(ptr) }.create_var())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeAnd(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
    b: jint,
) -> jint {
    guard(&mut env, 0, || unsafe { engine_mut(ptr) }.and(a, b))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeOr(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
    b: jint,
) -> jint {
    guard(&mut env, 0, || unsafe { engine_mut(ptr) }.or(a, b))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeDiff(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
    b: jint,
) -> jint {
    guard(&mut env, 0, || unsafe { engine_mut(ptr) }.diff(a, b))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeXor(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
    b: jint,
) -> jint {
    guard(&mut env, 0, || unsafe { engine_mut(ptr) }.xor(a, b))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeNot(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
) -> jint {
    guard(&mut env, 0, || unsafe { engine_mut(ptr) }.not(a))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeRef(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
) -> jint {
    guard(&mut env, a, || unsafe { engine_mut(ptr) }.add_ref(a))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeDeref(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
) -> jint {
    guard(&mut env, a, || unsafe { engine_mut(ptr) }.deref(a))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeGetRef(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
) -> jint {
    guard(&mut env, 0, || unsafe { engine_mut(ptr) }.get_ref(a))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeGc(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    guard(&mut env, 0, || unsafe { engine_mut(ptr) }.gc())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeSatCount(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
) -> jdouble {
    guard(&mut env, 0.0, || unsafe { engine_mut(ptr) }.sat_count(a))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeNodeCount(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    _a: jint,
) -> jint {
    // GCFLOBDD has no cheap per-handle node count; report the shared Context's
    // reachable node count (the diagnostic APKeep needs).
    guard(&mut env, 0, || unsafe { engine_mut(ptr) }.node_count())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeGetMemoryUsage(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    guard(&mut env, 0, || unsafe { engine_mut(ptr) }.memory_usage())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeIsValid(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
) -> jboolean {
    guard(&mut env, 0, || {
        jboolean::from(unsafe { engine_mut(ptr) }.is_valid(a))
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeNumVars(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    guard(&mut env, 0, || {
        unsafe { engine_mut(ptr) }.num_vars() as jint
    })
}

/// Fill `buffer` (length must equal `num_vars`) with one satisfying assignment
/// (`1`/`0`/`-1`) and return it, allocating a fresh array if `buffer` is null
/// or the wrong size. Mirrors jdd `int[] oneSat(int bdd, int[] buffer)`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeOneSat<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    ptr: jlong,
    a: jint,
    buffer: JIntArray<'local>,
) -> JIntArray<'local> {
    let null = JIntArray::default();
    let values = guard(&mut env, Vec::new(), || unsafe { engine_mut(ptr) }.one_sat(a));
    if values.is_empty() {
        return null;
    }

    // Reuse the caller's buffer when it is the right size, else allocate one.
    let out = match env.get_array_length(&buffer) {
        Ok(len) if len as usize == values.len() => buffer,
        _ => match env.new_int_array(values.len() as jint) {
            Ok(arr) => arr,
            Err(_) => return null,
        },
    };
    if env.set_int_array_region(&out, 0, &values).is_err() {
        return null;
    }
    out
}

/// Existential quantification (`exists(bdd, cube)`), used only by NAT rewriting.
/// Not yet implemented for GCFLOBDD (needs a canonicalizing reduction); the
/// fattree benchmark does not exercise it. Raises a Java exception if called.
/// Nodes reachable from this diagram's root. Unlike `nativeNodeCount` (which
/// reports the shared `Context`) this is per-handle, which is what `results/`
/// reports as diagram size. Node counts are convention-free.
#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeDiagramNodes(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
) -> jint {
    guard(&mut env, 0, || {
        unsafe { engine_mut(ptr) }.diagram_size(a).0 as jint
    })
}

/// Edges leaving this diagram's nodes, in the reference C++ CFLOBDD's counting
/// convention: two per connection plus every distinct return map's entries.
#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeDiagramEdges(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
) -> jint {
    guard(&mut env, 0, || {
        unsafe { engine_mut(ptr) }.diagram_size(a).1 as jint
    })
}

/// This diagram's nodes plus edges -- the single number the size tables quote,
/// in the reference C++ CFLOBDD's counting convention. Equal to
/// `nativeDiagramNodes + nativeDiagramEdges`; kept as its own entry point
/// because that sum is what `netbench` reports as `max_conv`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeConvTotal(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    a: jint,
) -> jint {
    guard(&mut env, 0, || {
        let (n, e) = unsafe { engine_mut(ptr) }.diagram_size(a);
        (n + e) as jint
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_application_wan_bdd_verifier_common_GcflobddEngine_nativeExists(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _bdd: jint,
    _cube: jint,
) -> jint {
    let _ = env.throw_new(
        "java/lang/UnsupportedOperationException",
        "exists/cofactor is not yet implemented in the GCFLOBDD backend",
    );
    0
}
