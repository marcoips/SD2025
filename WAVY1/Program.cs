using System;
using System.Net.Sockets;
using System.Text;

class Wavy
{
    private TcpClient aggregatorClient;
    private string wavyId;

    public Wavy(string aggregatorIp, int aggregatorPort, string id)
    {
        aggregatorClient = new TcpClient(aggregatorIp, aggregatorPort);
        wavyId = id;
    }

    public void Start()
    {
        NetworkStream stream = aggregatorClient.GetStream();
        string startMessage = $"INICIO: {wavyId}";
        byte[] buffer = Encoding.ASCII.GetBytes(startMessage);
        stream.Write(buffer, 0, buffer.Length);
        Console.WriteLine("Comunicação iniciada com o AGREGADOR.");
    }

    public void SendData(string data)
    {
        NetworkStream stream = aggregatorClient.GetStream();
        string message = $"{wavyId}:{data}";
        byte[] buffer = Encoding.ASCII.GetBytes(message);
        stream.Write(buffer, 0, buffer.Length);
        Console.WriteLine($"Dados enviados para o AGREGADOR: {data}");
    }

    public void Stop()
    {
        string endMessage = $"FIM:{wavyId}";
        byte[] buffer = Encoding.ASCII.GetBytes(endMessage);
        aggregatorClient.GetStream().Write(buffer, 0, buffer.Length);
        aggregatorClient.Close();
        Console.WriteLine("Comunicação finalizada.");
    }
}