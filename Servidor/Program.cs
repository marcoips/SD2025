using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class Server
{
    private TcpListener listener;
    private static Mutex mutex = new Mutex(); // Mutex para controle de acesso

    public Server(int port)
    {
        listener = new TcpListener(IPAddress.Any, port);
    }

    public void Start()
    {
        listener.Start();
        Console.WriteLine("Servidor iniciado...");
        while (true)
        {
            TcpClient client = listener.AcceptTcpClient();
            Thread clientThread = new Thread(HandleClient);
            clientThread.Start(client); // Passa o cliente como argumento
        }
    }

    private void HandleClient(object clientObj)
    {
        TcpClient client = (TcpClient)clientObj;
        NetworkStream stream = client.GetStream();
        byte[] buffer = new byte[1024];
        int bytesRead;

        while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) != 0)
        {
            string message = Encoding.ASCII.GetString(buffer, 0, bytesRead);
            Console.WriteLine("Recebido: " + message);


            mutex.WaitOne();
            try
            {

                Console.WriteLine("Processando dados...");
                Thread.Sleep(1000);
            }
            finally
            {
                mutex.ReleaseMutex();
            }
        }

        client.Close();
    }
}