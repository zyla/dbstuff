struct Var<M: ?Sized> {
    _phantom: std::marker::PhantomData<M>
}

impl<M> Var<M> {
    fn access(&self, _guard: &Guard<M>) {}
}

struct Guard<M: ?Sized> {
    _phantom: std::marker::PhantomData<M>
}

trait Mutex {
    fn new_var(&self) -> Var<Self>;
    fn lock(&self) -> Guard<Self>;
}

struct MutexImpl;

impl Mutex for MutexImpl {
    fn new_var(&self) -> Var<Self> {
        Var{_phantom: std::marker::PhantomData}
    }
    fn lock(&self) -> Guard<Self> {
        Guard{_phantom: std::marker::PhantomData}
    }
}

fn new_mutex() -> impl Mutex {
    MutexImpl
}

fn new_mutex_2() -> impl Mutex {
    MutexImpl
}

fn new_mutex_3() -> impl Mutex {
    if true {
        new_mutex()
    } else {
        new_mutex_2()
    }
}

fn main() {
    let m1 = new_mutex();
    let m2 = new_mutex_2();

    let v1 = m1.new_var();
    let v2 = m1.new_var();
    let v3 = m2.new_var();

    let guard1 = m1.lock();
    v1.access(&guard1);
    v2.access(&guard1);
}
