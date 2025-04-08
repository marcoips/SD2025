using System;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class Aggregator
{
    private TcpClient serverClient;
    private static Mutex mutex = new Mutex(); // Mutex para controle de acesso
    private DatabaseManager dbManager;

    public Aggregator(string serverIp, int serverPort, string dbFilePath)
    {
        serverClient = new TcpClient(serverIp, serverPort);
        dbManager = new DatabaseManager(dbFilePath);
    }

    public void ReceiveData(string data)
    {
        Console.WriteLine($"Dados recebidos da WAVY: {data}");

        // Simulação de pré-processamento
        string processedData = PreprocessData(data);

        // Acesso concorrente ao recurso
        mutex.WaitOne(); // Aguarda o acesso ao recurso
        try
        {
            // Armazenar dados na base de dados
            dbManager.InsertData("WAVY1", processedData); // Supondo que o ID da WAVY seja "WAVY1"
            Console.WriteLine("Dados armazenados na base de dados.");
        }
        finally
        {
            mutex.ReleaseMutex(); // Libera o acesso ao recurso
        }

        // Enviar dados processados para o SERVIDOR
        SendData("WAVY1", processedData); // Supondo que o ID da WAVY seja "WAVY1"
    }

    private string PreprocessData(string data)
    {
        return $"Processado: {data}";
    }

    public void SendData(string wavyId, string data)
    {
        NetworkStream stream = serverClient.GetStream();
        string message = $"INICIO:{wavyId}:{data}";
        byte[] buffer = Encoding.ASCII.GetBytes(message);
        stream.Write(buffer, 0, buffer.Length);
    }
}