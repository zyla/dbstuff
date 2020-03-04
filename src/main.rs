use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut listener = TcpListener::bind("0.0.0.0:8080").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(handle_client(socket));
    }
}

async fn handle_client(mut socket: TcpStream) -> io::Result<()> {
    let (read_stream, write_stream) = socket.split();
    let mut lines = BufReader::new(read_stream).lines();
    let mut output = BufWriter::new(write_stream);

    while let Some(line) = lines.next_line().await? {
        output.write_all(line.as_ref()).await?;
        output.write_all(b"\n").await?;
        output.flush().await?;
    }
    Ok(())
}
