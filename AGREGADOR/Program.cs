using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class AGREGADOR
{
    private TcpListener listener;
    private int port = 5000;

    public AGREGADOR()
    {
        listener = new TcpListener(IPAddress.Any, port);
    }

    public void Start()
    {
        listener.Start();
        Console.WriteLine("AGREGADOR iniciado...");
        while (true)
        {
            TcpClient client = listener.AcceptTcpClient();
            Thread clientThread = new Thread(HandleClient);
            clientThread.Start(client);
        }
    }

    private void HandleClient(object obj)
    {
        TcpClient client = (TcpClient)obj;
        NetworkStream stream = client.GetStream();
        byte[] buffer = new byte[256];
        int bytesRead = stream.Read(buffer, 0, buffer.Length);
        string message = Encoding.ASCII.GetString(buffer, 0, bytesRead);
        Console.WriteLine("Recebido: " + message);

        // Aqui você pode adicionar lógica para processar e encaminhar dados para o SERVIDOR

        client.Close();
    }
}
