use crate::config::Config;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};

pub struct AppState {
    pub config: Arc<Config>,
    mounts: Mutex<HashMap<String, Arc<ActiveMount>>>,
    client_count: AtomicUsize,
    source_count: AtomicUsize,
}

impl AppState {
    pub fn new(config: Config) -> Arc<Self> {
        Arc::new(Self {
            config: Arc::new(config),
            mounts: Mutex::new(HashMap::new()),
            client_count: AtomicUsize::new(0),
            source_count: AtomicUsize::new(0),
        })
    }

    pub fn register_source(self: &Arc<Self>, mount: &str) -> io::Result<SourceLease> {
        if self.source_count.load(Ordering::SeqCst) >= self.config.max_sources {
            return Err(io::Error::other("too many sources"));
        }

        let mut mounts = self.mounts.lock().expect("mount mutex poisoned");
        if mounts.contains_key(mount) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "mount already active",
            ));
        }

        let active = Arc::new(ActiveMount::new(mount.to_string()));
        mounts.insert(mount.to_string(), active.clone());
        self.source_count.fetch_add(1, Ordering::SeqCst);

        Ok(SourceLease {
            state: Arc::clone(self),
            mount: mount.to_string(),
            active,
        })
    }

    pub fn subscribe_client(self: &Arc<Self>, mount: &str) -> SubscribeResult {
        if self.client_count.load(Ordering::SeqCst) >= self.config.max_clients {
            return SubscribeResult::TooManyClients;
        }

        let active = {
            let mounts = self.mounts.lock().expect("mount mutex poisoned");
            mounts.get(mount).cloned()
        };

        let Some(active) = active else {
            return SubscribeResult::NoSuchMount;
        };

        if active.listener_count() >= self.config.max_clients_per_source {
            return SubscribeResult::TooManyClientsPerSource;
        }

        let rx = active.subscribe();
        self.client_count.fetch_add(1, Ordering::SeqCst);
        SubscribeResult::Subscribed {
            receiver: rx,
            lease: ClientLease {
                state: Arc::clone(self),
            },
        }
    }

    pub fn is_client_authorized(&self, mount: &str, user: &str, pass: &str) -> bool {
        match self.config.auth_mounts.get(mount) {
            Some(users) => users.get(user).is_some_and(|expected| expected == pass),
            None => true,
        }
    }

    pub fn mount_requires_auth(&self, mount: &str) -> bool {
        self.config.auth_mounts.contains_key(mount)
    }

    pub fn active_source_count(&self) -> usize {
        self.source_count.load(Ordering::SeqCst)
    }

    pub fn active_client_count(&self) -> usize {
        self.client_count.load(Ordering::SeqCst)
    }
}

pub enum SubscribeResult {
    Subscribed {
        receiver: Receiver<Vec<u8>>,
        lease: ClientLease,
    },
    NoSuchMount,
    TooManyClients,
    TooManyClientsPerSource,
}

pub struct ClientLease {
    state: Arc<AppState>,
}

impl Drop for ClientLease {
    fn drop(&mut self) {
        self.state.client_count.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct SourceLease {
    state: Arc<AppState>,
    mount: String,
    active: Arc<ActiveMount>,
}

impl SourceLease {
    pub fn broadcast(&self, bytes: &[u8]) {
        self.active.broadcast(bytes);
    }

    pub fn mount_name(&self) -> &str {
        &self.mount
    }

    pub fn listener_count(&self) -> usize {
        self.active.listener_count()
    }
}

impl Drop for SourceLease {
    fn drop(&mut self) {
        let mut mounts = self.state.mounts.lock().expect("mount mutex poisoned");
        mounts.remove(&self.mount);
        self.state.source_count.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct ActiveMount {
    _name: String,
    listeners: Mutex<Vec<Sender<Vec<u8>>>>,
}

impl ActiveMount {
    fn new(name: String) -> Self {
        Self {
            _name: name,
            listeners: Mutex::new(Vec::new()),
        }
    }

    fn subscribe(&self) -> Receiver<Vec<u8>> {
        let (tx, rx) = mpsc::channel();
        let mut listeners = self.listeners.lock().expect("listeners mutex poisoned");
        listeners.push(tx);
        rx
    }

    fn listener_count(&self) -> usize {
        let listeners = self.listeners.lock().expect("listeners mutex poisoned");
        listeners.len()
    }

    fn broadcast(&self, bytes: &[u8]) {
        let mut listeners = self.listeners.lock().expect("listeners mutex poisoned");
        let payload = bytes.to_vec();
        listeners.retain(|tx| tx.send(payload.clone()).is_ok());
    }
}
