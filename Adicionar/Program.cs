class Program
{
    static void Main(string[] args)
    {
        // Iniciar o servidor em uma thread separada
        Thread serverThread = new Thread(() =>
        {
            Server server = new Server(5000);
            server.Start();
        });
        serverThread.Start();

        // Criar e iniciar o AGREGADOR com o caminho do banco de dados
        string dbFilePath = "WavyData.db"; // Caminho do arquivo do banco de dados
        Aggregator aggregator = new Aggregator("127.0.0.1", 5000, dbFilePath);

        // Criar e iniciar a WAVY
        Wavy wavy = new Wavy("127.0.0.1", 5000, "WAVY1");
        wavy.Start();

        // Simular o envio de dados da WAVY para o AGREGADOR
        wavy.SendData("Dados de exemplo 1");
        aggregator.ReceiveData("Dados de exemplo 1"); // Simulando a recepção de dados

        // Finalizar a comunicação
        wavy.Stop();
    }
}