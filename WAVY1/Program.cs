using System;
using System.Net.Sockets;
using System.Text;

class WAVY
{
    private string aggregatorIp;
    private int aggregatorPort = 5000; // Porta padrão do agregador

    public WAVY(string ip)
    {
        aggregatorIp = ip;
    }

    public void Connect()
    {
        using (TcpClient client = new TcpClient(aggregatorIp, aggregatorPort))
        {
            NetworkStream stream = client.GetStream();
            string message = "WAVY_ID:register";
            byte[] data = Encoding.ASCII.GetBytes(message);
            stream.Write(data, 0, data.Length);
            Console.WriteLine("Mensagem enviada: " + message);
        }
    }

    // Método para enviar dados
    public void SendData(string data)
    {
        using (TcpClient client = new TcpClient(aggregatorIp, aggregatorPort))
        {
            NetworkStream stream = client.GetStream();
            byte[] dataBytes = Encoding.ASCII.GetBytes(data);
            stream.Write(dataBytes, 0, dataBytes.Length);
            Console.WriteLine("Dados enviados: " + data);
        }
    }
}
