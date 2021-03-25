use std::{
    fs::File,
    process::{Child, Command},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Mutex;

type Result<T = (), E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

#[tokio::test]
async fn new_server_needs_onboarded() -> Result {
    let server_fixture = ServerFixture::create_single_use().await;
    let client = server_fixture.client();

    let res = client.is_onboarding_allowed().await?;
    assert!(res);
    Ok(())
}

#[tokio::test]
async fn onboarding() -> Result {
    let server_fixture = ServerFixture::create_single_use().await;
    let client = server_fixture.client();

    let username = "some-user";
    let org = "some-org";
    let bucket = "some-bucket";
    let password = "some-password";
    let retention_period_hrs = 0;

    client
        .onboarding(
            username,
            org,
            bucket,
            Some(password.to_string()),
            Some(retention_period_hrs),
            None,
        )
        .await?;

    let res = client.is_onboarding_allowed().await?;
    assert!(!res);
    Ok(())
}

/// Represents a server that has been started and is available for
/// testing.
pub struct ServerFixture {
    server: Arc<TestServer>,
}

impl ServerFixture {
    /// Create a new server fixture and wait for it to be ready. This
    /// is called "create" rather than new because it is async and
    /// waits.  The database is left unconfigured and
    /// is not shared with any other tests.
    pub async fn create_single_use() -> Self {
        let server = TestServer::new().expect("Could start test server");
        let server = Arc::new(server);

        // ensure the server is ready
        server.wait_until_ready().await;
        Self { server }
    }

    /// Return a client suitable for communicating with this server
    pub fn client(&self) -> influxdb2_client::Client {
        influxdb2_client::Client::new(self.http_base(), "")
    }

    /// Return the http base URL for the HTTP API
    pub fn http_base(&self) -> &str {
        &self.server.http_base
    }
}

// These port numbers are chosen to not collide with a development ioxd/influxd
// server running locally.
// TODO(786): allocate random free ports instead of hardcoding.
// TODO(785): we cannot use localhost here.
static NEXT_PORT: AtomicUsize = AtomicUsize::new(8190);

#[derive(Debug)]
/// Represents the current known state of a TestServer
enum ServerState {
    Started,
    Ready,
    Error,
}

struct TestServer {
    /// Is the server ready to accept connections?
    ready: Mutex<ServerState>,
    /// Handle to the server process being controlled
    server_process: Child,
    /// HTTP API base
    http_base: String,
}

impl TestServer {
    fn new() -> Result<Self> {
        let ready = Mutex::new(ServerState::Started);
        let http_port = NEXT_PORT.fetch_add(1, SeqCst);
        let http_base = format!("http://127.0.0.1:{}", http_port);

        // Make a log file and bolt db in the temporary dir (don't auto delete it to
        // help debugging efforts)
        let temp_dir = std::env::temp_dir();

        let mut log_path = temp_dir.clone();
        log_path.push(format!("influxdb_server_fixture_{}.log", http_port));

        let mut bolt_path = temp_dir;
        bolt_path.push(format!("influxd{}.bolt", http_port));

        println!("****************");
        println!("Server Logging to {:?}", log_path);
        println!("****************");
        let log_file = File::create(log_path).expect("Opening log file");

        let stdout_log_file = log_file
            .try_clone()
            .expect("cloning file handle for stdout");
        let stderr_log_file = log_file;

        let server_process = Command::new("influxd")
            .arg("--http-bind-address")
            .arg(format!(":{}", http_port))
            .arg("--bolt-path")
            .arg(bolt_path)
            // redirect output to log file
            .stdout(stdout_log_file)
            .stderr(stderr_log_file)
            .spawn()?;

        Ok(Self {
            ready,
            server_process,
            http_base,
        })
    }

    async fn wait_until_ready(&self) {
        let mut ready = self.ready.lock().await;
        match *ready {
            ServerState::Started => {} // first time, need to try and start it
            ServerState::Ready => {
                return;
            }
            ServerState::Error => {
                panic!("Server was previously found to be in Error, aborting");
            }
        }

        let try_http_connect = async {
            let client = reqwest::Client::new();
            let url = format!("{}/health", self.http_base);
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                match client.get(&url).send().await {
                    Ok(resp) => {
                        println!("Successfully got a response from HTTP: {:?}", resp);
                        return;
                    }
                    Err(e) => {
                        println!("Waiting for HTTP server to be up: {}", e);
                    }
                }
                interval.tick().await;
            }
        };

        let capped_check = tokio::time::timeout(Duration::from_secs(15), try_http_connect);

        match capped_check.await {
            Ok(_) => {
                println!("Successfully started {}", self);
                *ready = ServerState::Ready;
            }
            Err(e) => {
                // tell others that this server had some problem
                *ready = ServerState::Error;
                std::mem::drop(ready);
                panic!("Server was not ready in required time: {}", e);
            }
        }
    }
}

impl std::fmt::Display for TestServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "TestServer (http api: {})", self.http_base)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server_process
            .kill()
            .expect("Should have been able to kill the test server");
    }
}
