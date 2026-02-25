use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Config {
    pub ports: Vec<u16>,
    pub max_clients: usize,
    pub max_clients_per_source: usize,
    pub max_sources: usize,
    pub encoder_password: String,
    pub server_name: String,
    pub location: String,
    pub rp_email: String,
    pub server_url: String,
    pub logdir: String,
    pub logfile: String,
    pub config_path: PathBuf,
    pub sourcetable_path: PathBuf,
    pub auth_mounts: HashMap<String, HashMap<String, String>>,
}

impl Default for Config {
    fn default() -> Self {
        let config_path = PathBuf::from("ntripcaster/conf/ntripcaster.conf");
        let sourcetable_path = PathBuf::from("ntripcaster/conf/sourcetable.dat");
        Self {
            ports: vec![2101],
            max_clients: 100,
            max_clients_per_source: 100,
            max_sources: 40,
            encoder_password: "sesam01".to_string(),
            server_name: "localhost".to_string(),
            location: "BKG Geodetic Department".to_string(),
            rp_email: "euref-ip@bkg.bund.de".to_string(),
            server_url: "http://igs.ifag.de/".to_string(),
            logdir: ".".to_string(),
            logfile: "ntripcaster.log".to_string(),
            config_path,
            sourcetable_path,
            auth_mounts: HashMap::new(),
        }
    }
}

impl Config {
    pub fn load_from_cli() -> io::Result<Self> {
        let cli_path = parse_cli_config_path(env::args().skip(1))?;
        let env_path = env::var_os("NTRIPCASTER_CONFIG").map(PathBuf::from);
        let requested = cli_path.or(env_path);

        let mut config = Self::default();
        let resolved = match requested {
            Some(path) => path,
            None => default_config_candidates()
                .into_iter()
                .find(|p| p.exists())
                .unwrap_or_else(|| config.config_path.clone()),
        };
        config.config_path = resolved.clone();

        if resolved.exists() {
            config.apply_file(&resolved)?;
        }

        config.sourcetable_path = resolve_sourcetable_path(&resolved);
        Ok(config)
    }

    fn apply_file(&mut self, path: &Path) -> io::Result<()> {
        let contents = fs::read_to_string(path)?;
        self.ports.clear();
        for raw_line in contents.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if line.starts_with('/') {
                self.parse_mount_auth(line);
                continue;
            }

            let mut parts = line.splitn(2, char::is_whitespace);
            let key = parts.next().unwrap_or_default().trim();
            let value = parts.next().unwrap_or_default().trim();
            if key.is_empty() || value.is_empty() {
                continue;
            }

            match key {
                "port" => {
                    if let Ok(port) = value.parse::<u16>() {
                        self.ports.push(port);
                    }
                }
                "encoder_password" => self.encoder_password = value.to_string(),
                "max_clients" => {
                    if let Ok(v) = value.parse::<usize>() {
                        self.max_clients = v;
                    }
                }
                "max_clients_per_source" => {
                    if let Ok(v) = value.parse::<usize>() {
                        self.max_clients_per_source = v;
                    }
                }
                "max_sources" => {
                    if let Ok(v) = value.parse::<usize>() {
                        self.max_sources = v;
                    }
                }
                "server_name" => self.server_name = value.to_string(),
                "location" => self.location = value.to_string(),
                "rp_email" => self.rp_email = value.to_string(),
                "server_url" => self.server_url = value.to_string(),
                "logdir" => self.logdir = value.to_string(),
                "logfile" => self.logfile = value.to_string(),
                _ => {}
            }
        }

        if self.ports.is_empty() {
            self.ports.push(2101);
        }

        Ok(())
    }

    fn parse_mount_auth(&mut self, line: &str) {
        let Some((mount, rest)) = line.split_once(':') else {
            // Legacy config allows bare mount lines, which indicate no auth.
            return;
        };
        let mount = mount.trim();
        if mount.is_empty() {
            return;
        }

        let mut users = HashMap::new();
        for user_spec in rest.split(',') {
            let spec = user_spec.trim();
            if spec.is_empty() {
                continue;
            }
            if let Some((user, pass)) = spec.split_once(':') {
                let user = user.trim();
                let pass = pass.trim();
                if !user.is_empty() {
                    users.insert(user.to_string(), pass.to_string());
                }
            }
        }

        if !users.is_empty() {
            self.auth_mounts.insert(mount.to_string(), users);
        }
    }
}

fn parse_cli_config_path<I>(mut args: I) -> io::Result<Option<PathBuf>>
where
    I: Iterator<Item = String>,
{
    let mut config_path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-c" | "--config" => {
                let Some(path) = args.next() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --config",
                    ));
                };
                config_path = Some(PathBuf::from(path));
            }
            _ => {}
        }
    }
    Ok(config_path)
}

fn default_config_candidates() -> Vec<PathBuf> {
    vec![
        PathBuf::from("ntripcaster/conf/ntripcaster.conf"),
        PathBuf::from("ntripcaster/conf/ntripcaster.conf.dist"),
        PathBuf::from("ntripcaster.conf"),
    ]
}

fn resolve_sourcetable_path(config_path: &Path) -> PathBuf {
    let dir = config_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let primary = dir.join("sourcetable.dat");
    if primary.exists() {
        return primary;
    }
    let dist = dir.join("sourcetable.dat.dist");
    if dist.exists() {
        return dist;
    }
    primary
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn parses_mount_auth_line() {
        let mut cfg = Config::default();
        cfg.parse_mount_auth("/MOUNT:user1:pass1,user2:pass2");
        let users = cfg.auth_mounts.get("/MOUNT").expect("mount present");
        assert_eq!(users.get("user1").map(String::as_str), Some("pass1"));
        assert_eq!(users.get("user2").map(String::as_str), Some("pass2"));
    }

    #[test]
    fn ignores_open_mount_lines() {
        let mut cfg = Config::default();
        cfg.parse_mount_auth("/OPENMOUNT");
        assert!(!cfg.auth_mounts.contains_key("/OPENMOUNT"));
    }
}
