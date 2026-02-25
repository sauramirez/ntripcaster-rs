use crate::config::Config;
use crate::state::{AppState, SubscribeResult};
use smoltcp::wire::IpListenEndpoint;
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task;
use tokio::time::sleep;

pub async fn run(config: Config) -> io::Result<()> {
    let state = AppState::new(config);

    eprintln!(
        "ntripcaster-rs starting: ports={:?} config={} sourcetable={}",
        state.config.ports,
        state.config.config_path.display(),
        state.config.sourcetable_path.display()
    );

    let mut bound = 0usize;
    for port in state.config.ports.clone() {
        let listener = TcpListener::bind(("0.0.0.0", port)).await?;
        let listen_endpoint: IpListenEndpoint = port.into();
        eprintln!("listening on {listen_endpoint}");
        bound += 1;

        let shared = Arc::clone(&state);
        tokio::spawn(accept_loop(listener, shared));
    }

    if bound == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no ports configured",
        ));
    }

    loop {
        sleep(Duration::from_secs(60)).await;
        eprintln!(
            "status: {} source(s), {} client(s)",
            state.active_source_count(),
            state.active_client_count()
        );
    }
}

async fn accept_loop(listener: TcpListener, state: Arc<AppState>) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let shared = Arc::clone(&state);
                task::spawn_blocking(move || {
                    let stream = match stream.into_std() {
                        Ok(stream) => stream,
                        Err(err) => {
                            eprintln!("connection {addr}: failed to convert tokio stream: {err}");
                            return;
                        }
                    };
                    if let Err(err) = stream.set_nonblocking(false) {
                        eprintln!("connection {addr}: failed to set blocking mode: {err}");
                        return;
                    }
                    if let Err(err) = handle_connection(stream, addr, shared) {
                        eprintln!("connection {addr}: {err}");
                    }
                });
            }
            Err(err) => {
                eprintln!("accept error: {err}");
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

fn handle_connection(stream: TcpStream, addr: SocketAddr, state: Arc<AppState>) -> io::Result<()> {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));

    let mut reader = BufReader::new(stream);
    let req = match read_request(&mut reader)? {
        Some(req) => req,
        None => return Ok(()),
    };

    eprintln!(
        "connection {addr}: {} {}",
        req.first_line,
        req.headers
            .get("user-agent")
            .unwrap_or(&"<no user-agent>".to_string())
    );
    if req.first_line.starts_with("GET ") {
        handle_client_get(reader.into_inner(), req, addr, state)
    } else if req.first_line.starts_with("SOURCE ") {
        handle_source(reader, req, addr, state)
    } else if req.first_line.starts_with("POST ") {
        handle_source_post(reader, req, addr, state)
    } else {
        let mut stream = reader.into_inner();
        write_400(&mut stream)?;
        Ok(())
    }
}

struct RequestHead {
    first_line: String,
    headers: HashMap<String, String>,
}

fn read_request(reader: &mut BufReader<TcpStream>) -> io::Result<Option<RequestHead>> {
    let first_line_deadline = Instant::now() + Duration::from_secs(60);
    let mut first_line = String::new();
    let n = loop {
        match reader.read_line(&mut first_line) {
            Ok(n) => break n,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if Instant::now() >= first_line_deadline {
                    eprintln!("request read timeout before first line (deadline exceeded)");
                    return Ok(None);
                }
                first_line.clear();
                continue;
            }
            Err(err) => {
                eprintln!("request read error before first line: {err}");
                return Err(err);
            }
        }
    };
    if n == 0 {
        return Ok(None);
    }
    trim_line_ending(&mut first_line);
    if first_line.is_empty() {
        return Ok(None);
    }

    if first_line.starts_with("SOURCE ") {
        let _ = reader
            .get_mut()
            .set_read_timeout(Some(Duration::from_millis(20)));
    } else if first_line.starts_with("POST ") {
        let _ = reader
            .get_mut()
            .set_read_timeout(Some(Duration::from_millis(200)));
    }

    let mut headers = HashMap::new();
    let mut total_bytes = first_line.len();
    loop {
        let mut line = String::new();
        let n = match reader.read_line(&mut line) {
            Ok(n) => n,
            // Some NTRIP sources (including str2str variants) do not always send a
            // fully HTTP-style header terminator before streaming data. Treat a read
            // timeout here as "headers finished" instead of failing the connection.
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(err) => return Err(err),
        };
        if n == 0 {
            break;
        }
        total_bytes += n;
        if total_bytes > 16 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header too large",
            ));
        }
        trim_line_ending(&mut line);
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }

    Ok(Some(RequestHead {
        first_line,
        headers,
    }))
}

fn handle_client_get(
    mut stream: TcpStream,
    req: RequestHead,
    addr: SocketAddr,
    state: Arc<AppState>,
) -> io::Result<()> {
    let path = parse_get_path(&req.first_line).unwrap_or_else(|| "/".to_string());
    if path == "/" || path.is_empty() {
        send_sourcetable(&mut stream, &state.config)?;
        return Ok(());
    }

    if !header_starts_with(&req.headers, "user-agent", "ntrip") {
        write_401(&mut stream, &path)?;
        return Ok(());
    }

    if state.mount_requires_auth(&path) {
        let auth = req
            .headers
            .get("authorization")
            .and_then(|v| parse_basic_authorization(v));
        let Some((user, pass)) = auth else {
            write_401(&mut stream, &path)?;
            return Ok(());
        };
        if !state.is_client_authorized(&path, &user, &pass) {
            write_401(&mut stream, &path)?;
            return Ok(());
        }
    }

    match state.subscribe_client(&path) {
        SubscribeResult::NoSuchMount => {
            send_sourcetable(&mut stream, &state.config)?;
            Ok(())
        }
        SubscribeResult::TooManyClients | SubscribeResult::TooManyClientsPerSource => {
            write_text_line(&mut stream, "ERROR - Server Full")?;
            Ok(())
        }
        SubscribeResult::Subscribed { receiver, lease } => {
            eprintln!("client {addr} subscribing to {path} successfully");
            greet_client(&mut stream)?;
            eprintln!("client {addr} subscribed to {path}");
            let _lease = lease;
            for chunk in receiver {
                if let Err(err) = stream.write_all(&chunk) {
                    eprintln!("client {addr} write error on {path}: {err}");
                    break;
                }
            }
            Ok(())
        }
    }
}

fn handle_source(
    mut reader: BufReader<TcpStream>,
    req: RequestHead,
    addr: SocketAddr,
    state: Arc<AppState>,
) -> io::Result<()> {
    eprintln!(
        "source {addr} request: first_line={:?} headers={:?}",
        req.first_line, req.headers
    );

    let (password, mount) = match parse_source_line(&req.first_line) {
        Some(v) => v,
        None => {
            let mut stream = reader.into_inner();
            write_text_line(&mut stream, "ERROR - Missing Mountpoint")?;
            return Ok(());
        }
    };

    if password != state.config.encoder_password {
        let mut stream = reader.into_inner();
        write_text_line(&mut stream, "ERROR - Bad Password")?;
        return Ok(());
    }

    let source_agent_ok = header_starts_with(&req.headers, "source-agent", "ntrip")
        || header_starts_with(&req.headers, "user-agent", "ntrip");
    if !source_agent_ok {
        eprintln!("source {addr} continuing without NTRIP agent header for compatibility");
    }

    let source = match state.register_source(&mount) {
        Ok(source) => source,
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            let mut stream = reader.into_inner();
            write_text_line(&mut stream, "ERROR - Mount Point Taken or Invalid")?;
            return Ok(());
        }
        Err(_) => {
            let mut stream = reader.into_inner();
            write_text_line(&mut stream, "ERROR - Too many sources")?;
            return Ok(());
        }
    };

    {
        let stream = reader.get_mut();
        write_text_line(stream, "OK")?;
    }

    eprintln!("source {addr} mounted {}", source.mount_name());

    let buffered_leftover = {
        let leftover = reader.buffer().to_vec();
        let len = leftover.len();
        reader.consume(len);
        leftover
    };
    if !buffered_leftover.is_empty() {
        source.broadcast(&buffered_leftover);
    }

    let mut stream = reader.into_inner();
    let _ = stream.set_read_timeout(None);
    let _ = stream.set_write_timeout(None);
    let mut buf = [0_u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                eprintln!("source {mount} read: {} bytes", buf.len());
                source.broadcast(&buf[..n])
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) if err.kind() == io::ErrorKind::TimedOut => continue,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => {
                eprintln!("source {addr} read error on {}: {err}", source.mount_name());
                break;
            }
        }
    }

    eprintln!(
        "source {addr} disconnected {} ({} listeners)",
        source.mount_name(),
        source.listener_count()
    );
    Ok(())
}

fn handle_source_post(
    mut reader: BufReader<TcpStream>,
    req: RequestHead,
    addr: SocketAddr,
    state: Arc<AppState>,
) -> io::Result<()> {
    eprintln!(
        "source {addr} POST request: first_line={:?} headers={:?}",
        req.first_line, req.headers
    );

    let mount = match parse_post_path(&req.first_line) {
        Some(mount) => mount,
        None => {
            let mut stream = reader.into_inner();
            write_400(&mut stream)?;
            return Ok(());
        }
    };

    let auth = req
        .headers
        .get("authorization")
        .and_then(|v| parse_basic_authorization(v));
    let Some((_user, pass)) = auth else {
        let mut stream = reader.into_inner();
        write_401(&mut stream, &mount)?;
        return Ok(());
    };
    if pass != state.config.encoder_password {
        let mut stream = reader.into_inner();
        write_401(&mut stream, &mount)?;
        return Ok(());
    }

    let source = match state.register_source(&mount) {
        Ok(source) => source,
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            let mut stream = reader.into_inner();
            write_text_line(&mut stream, "ERROR - Mount Point Taken or Invalid")?;
            return Ok(());
        }
        Err(_) => {
            let mut stream = reader.into_inner();
            write_text_line(&mut stream, "ERROR - Too many sources")?;
            return Ok(());
        }
    };

    {
        let stream = reader.get_mut();
        write_post_source_ok(stream)?;
    }

    eprintln!("source {addr} mounted {} via POST", source.mount_name());

    let buffered_leftover = {
        let leftover = reader.buffer().to_vec();
        let len = leftover.len();
        reader.consume(len);
        leftover
    };
    if !buffered_leftover.is_empty() {
        source.broadcast(&buffered_leftover);
    }

    let mut stream = reader.into_inner();
    let _ = stream.set_read_timeout(None);
    let _ = stream.set_write_timeout(None);
    let mut buf = [0_u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                eprintln!("source {mount} POST read: {} bytes", buf.len());
                source.broadcast(&buf[..n])
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) if err.kind() == io::ErrorKind::TimedOut => continue,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => {
                eprintln!(
                    "source {addr} POST read error on {}: {err}",
                    source.mount_name()
                );
                break;
            }
        }
    }

    eprintln!(
        "source {addr} disconnected {} via POST ({} listeners)",
        source.mount_name(),
        source.listener_count()
    );
    Ok(())
}

fn parse_get_path(first_line: &str) -> Option<String> {
    let mut parts = first_line.split_whitespace();
    let method = parts.next()?;
    if method != "GET" {
        return None;
    }
    let path = parts.next().unwrap_or("/");
    Some(path.to_string())
}

fn parse_source_line(first_line: &str) -> Option<(String, String)> {
    let mut parts = first_line.split_whitespace();
    if !parts.next()?.eq_ignore_ascii_case("SOURCE") {
        return None;
    }
    let pass = parts.next()?.to_string();
    let mount = parts.next()?.trim().to_string();
    if mount.is_empty() {
        return None;
    }
    let mount = if mount.starts_with('/') {
        mount
    } else {
        format!("/{mount}")
    };
    Some((pass, mount))
}

fn parse_post_path(first_line: &str) -> Option<String> {
    let mut parts = first_line.split_whitespace();
    if !parts.next()?.eq_ignore_ascii_case("POST") {
        return None;
    }
    let path = parts.next()?.trim();
    if path.is_empty() {
        return None;
    }
    Some(if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    })
}

fn header_starts_with(headers: &HashMap<String, String>, key: &str, prefix: &str) -> bool {
    headers.get(key).is_some_and(|value| {
        value
            .to_ascii_lowercase()
            .starts_with(&prefix.to_ascii_lowercase())
    })
}

fn greet_client(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(b"ICY 200 OK\r\n")
}

fn send_sourcetable(stream: &mut TcpStream, config: &Config) -> io::Result<()> {
    let data = fs::read(&config.sourcetable_path)
        .unwrap_or_else(|_| b"NO SOURCETABLE AVAILABLE\n".to_vec());
    let mut body = data;
    if !body.ends_with(b"\n") {
        body.push(b'\n');
    }
    body.extend_from_slice(b"ENDSOURCETABLE\r\n");

    write!(
        stream,
        "SOURCETABLE 200 OK\r\nServer: NTRIP NtripCaster-rs 0.1/1.0\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)
}

fn write_400(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(
        b"HTTP/1.0 400 Bad Request\r\nServer: NTRIP NtripCaster-rs 0.1/1.0\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n",
    )
}

fn write_401(stream: &mut TcpStream, realm: &str) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.0 401 Unauthorized\r\nServer: NTRIP NtripCaster-rs 0.1/1.0\r\nWWW-Authenticate: Basic realm=\"{}\"\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n",
        realm
    )
}

fn write_post_source_ok(stream: &mut TcpStream) -> io::Result<()> {
    eprintln!("responding to POST source with HTTP 200 OK");
    stream.write_all(
        b"HTTP/1.1 200 OK\r\nServer: NTRIP NtripCaster-rs 0.1/2.0\r\nNtrip-Version: Ntrip/2.0\r\nConnection: close\r\n\r\n",
    )
}

fn write_text_line(stream: &mut TcpStream, line: &str) -> io::Result<()> {
    stream.write_all(line.as_bytes())?;
    stream.write_all(b"\r\n")
}

fn trim_line_ending(s: &mut String) {
    while s.ends_with(['\r', '\n']) {
        s.pop();
    }
}

fn parse_basic_authorization(value: &str) -> Option<(String, String)> {
    let (scheme, encoded) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Basic") {
        return None;
    }
    let decoded = decode_base64(encoded.trim())?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (user, pass) = decoded.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

fn decode_base64(input: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity((input.len() * 3) / 4);
    let mut buf = [0_u8; 4];
    let mut buf_len = 0usize;

    for b in input.bytes() {
        let v = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => 64,
            b' ' | b'\r' | b'\n' | b'\t' => continue,
            _ => return None,
        };
        buf[buf_len] = v;
        buf_len += 1;

        if buf_len == 4 {
            if buf[0] == 64 || buf[1] == 64 {
                return None;
            }
            out.push((buf[0] << 2) | (buf[1] >> 4));
            if buf[2] != 64 {
                out.push((buf[1] << 4) | (buf[2] >> 2));
            }
            if buf[3] != 64 {
                out.push((buf[2] << 6) | buf[3]);
            }
            buf_len = 0;
        }
    }

    if buf_len != 0 {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::{decode_base64, parse_basic_authorization};

    #[test]
    fn decodes_base64() {
        let decoded = decode_base64("dXNlcjpwYXNz").expect("decode");
        assert_eq!(decoded, b"user:pass");
    }

    #[test]
    fn parses_basic_auth() {
        let parsed = parse_basic_authorization("Basic dXNlcjpwYXNz").expect("auth");
        assert_eq!(parsed.0, "user");
        assert_eq!(parsed.1, "pass");
    }
}
