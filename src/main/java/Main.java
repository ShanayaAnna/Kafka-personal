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
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 9092;

    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true); // Since the tester restarts your program quite often, setting SO_REUSEADDR ensures that we don't run into 'Address already in use' errors

      // Wait for connection from client.
      clientSocket = serverSocket.accept();

      InputStream in = clientSocket.getInputStream();
      byte[] message_size = in.readNBytes(4);
      byte[] request_api_key = in.readNBytes(2);
      byte[] request_api_version = in.readNBytes(2);
      byte[] correlation_id = in.readNBytes(4);

      OutputStream out = clientSocket.getOutputStream();
      ByteArrayOutputStream bout = new ByteArrayOutputStream();

      bout.write(correlation_id);

      int version = ByteBuffer.wrap(request_api_version).getShort();
      if (version<0 || version>4) bout.write(new byte[] {0,35});
      else {
        bout.write(new byte[] {0, 0});       // error code
        bout.write(2);                       // array size + 1
        bout.write(new byte[] {0, 18});      // api_key
        bout.write(new byte[] {0, 3});       // min version
        bout.write(new byte[] {0, 4});       // max version
        bout.write(0);                       // tagged fields
        bout.write(new byte[] {0, 0, 0, 0}); // throttle time
        bout.write(0); // tagged fields
      }

      int size = bout.size();
      byte[] sizeBytes = ByteBuffer.allocate(4).putInt(size).array();
      var response = bout.toByteArray();
      System.out.println(Arrays.toString(sizeBytes));
      System.out.println(Arrays.toString(response));
      out.write(sizeBytes);
      out.write(response);

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}
