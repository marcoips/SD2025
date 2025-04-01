using System;
using System.Data;
using System.Net.Sockets;
using System.Text;
using System.Threading;

public class WavyTemperatura
{
    private string _agregadorIp;
    private int _agregadorPorta;
    private string _wavyId;
    private TcpClient _client;
    private NetworkStream _stream;
    private Timer _timerEnvio;

    public WavyTemperatura(string agregadorIp, int agregadorPorta, string wavyId)
    {
        _agregadorIp = agregadorIp;
        _agregadorPorta = agregadorPorta;
        _wavyId = wavyId;
        _client = new TcpClient();
    }

    // Inicia a conexão com o AGREGADOR
    public void Iniciar()
    {
        try
        {
            Console.WriteLine($"Espera...");
            _client.Connect(_agregadorIp, _agregadorPorta);
            _stream = _client.GetStream();
            Console.Clear();
            Console.WriteLine($"Conectado ao AGREGADOR em {_agregadorIp}:{_agregadorPorta}");

            // Registra a WAVY no AGREGADOR
            RegistrarWavy();

            // Configura o temporizador para envio de dados a cada 1 minuto
            _timerEnvio = new Timer(EnviarDados, null, TimeSpan.Zero, TimeSpan.FromMinutes(1));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro: {ex.Message}");
        }
    }

    // Envia mensagem de registro (REGISTER)
    private void RegistrarWavy()
    {
        string mensagem = $"REGISTER:{_wavyId}:TEMP";
        byte[] buffer = Encoding.ASCII.GetBytes(mensagem);
        _stream.Write(buffer, 0, buffer.Length);
        Console.WriteLine("Registro enviado. Aguardando confirmação...");

        // Aguarda resposta (REGISTER_OK)
        byte[] respostaBuffer = new byte[1024];
        int bytesLidos = _stream.Read(respostaBuffer, 0, respostaBuffer.Length);
        string resposta = Encoding.ASCII.GetString(respostaBuffer, 0, bytesLidos);

        if (resposta.StartsWith("REGISTER_OK"))
            Console.WriteLine("Registro confirmado pelo AGREGADOR!");
        else
            Console.WriteLine($"Erro no registro: {resposta}");
    }

    // Simula a medição de temperatura e envia dados (DATA)
    private void EnviarDados(object state)
    {
        try
        {
            // Simula uma leitura de temperatura (0°C a 30°C)
            Random rand = new Random();
            double temperatura = Math.Round(rand.NextDouble() * 30, 2);
            string timestamp = DateTime.Now.ToString("yyyyMMddHHmm");

            // Formata a mensagem DATA
            string mensagem = $"DATA:{_wavyId}:TEMP:{timestamp}:{temperatura}";
            byte[] buffer = Encoding.ASCII.GetBytes(mensagem);
            _stream.Write(buffer, 0, buffer.Length);
            Console.WriteLine($"Dados enviados: {mensagem}");

            // Aguarda confirmação (DATA_ACK)
            byte[] respostaBuffer = new byte[1024];
            int bytesLidos = _stream.Read(respostaBuffer, 0, respostaBuffer.Length);
            string resposta = Encoding.ASCII.GetString(respostaBuffer, 0, bytesLidos);

            if (resposta.Contains("CACHE_CLEAR"))
                Console.WriteLine("Dados processados. Cache liberado.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao enviar dados: {ex.Message}");
        }
    }

    // Encerra a conexão
    public void Parar()
    {
        _timerEnvio?.Dispose();
        _stream?.Close();
        _client?.Close();
        Console.WriteLine("Conexão encerrada.");
    }
}

// Exemplo de uso
public class Program
{
    public static void Main()
    {
        WavyTemperatura wavy = new WavyTemperatura("192.168.1.100", 8080, "WAVY_TEMP_001");
        wavy.Iniciar();

        Console.WriteLine("Pressione ENTER para parar...");
        Console.ReadLine();
        wavy.Parar();
    }
}