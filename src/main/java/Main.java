import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

public class Main {
  public static void main(String[] args){
    System.err.println("Logs from your program will appear here!");
    int port = 9092;

    try(ServerSocket serverSocket = new ServerSocket(port)) {
      serverSocket.setReuseAddress(true);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        System.err.println("New client connected");

        new Thread(() -> handleClient(clientSocket)).start();

      }
    } catch (IOException e) {
      System.err.println("IOException: "+e.getMessage());
    }

  }
  private static void handleClient(Socket clientSocket) {
    try(InputStream in = clientSocket.getInputStream(); OutputStream out = clientSocket.getOutputStream()){
      while (true) {
        byte[] message_size = in.readNBytes(4);

        // Read the entire request based on message size
        byte[] request = in.readNBytes(ByteBuffer.wrap(message_size).getInt());
        byte[] request_api_key = Arrays.copyOfRange(request, 0, 2);
        byte[] request_api_version = Arrays.copyOfRange(request, 2, 4);
        byte[] correlation_id = Arrays.copyOfRange(request, 4, 8);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        bout.write(correlation_id);

        int version = ByteBuffer.wrap(request_api_version).getShort();
        if (version < 0 || version > 4) {
          bout.write(new byte[]{0, 35}); // Error code
        } else {
          bout.write(new byte[]{0, 0});       // Error code
          bout.write(2);                    // Array size + 1
          bout.write(request_api_key);        // Api Key
          bout.write(new byte[]{0, 3});       // Min Version
          bout.write(request_api_version);    // Max Version
          bout.write(0);                    // Tagged fields
          bout.write(new byte[]{0, 0, 0, 0}); // Throttle time
          bout.write(0);                    // Tagged fields
        }

        int response_size = bout.size();
        byte[] response_size_bytes = ByteBuffer.allocate(4).putInt(response_size).array();
        out.write(response_size_bytes);
        out.write(bout.toByteArray());
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) clientSocket.close();
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}
