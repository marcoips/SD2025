// Bibliotecas principais
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;

namespace WavyTempApp
{
    class Program
    {
        // Configuração da ligação
        private const string AGG_IP = "127.0.0.1";        // IP do Agregador
        private const int AGG_PORT = 5000;                // Porto do Agregador
        private const string WAVY_ID = "WAVY001";          // Identificador único da WAVY
        private const string WAVY_STATE = "operacao";      // Estado inicial do dispositivo

        private static readonly TimeSpan SEND_INTERVAL = TimeSpan.FromSeconds(10); // Intervalo entre envios
        private const int WATCHDOG_MS = 5_000;                                     // Intervalo do watchdog

        private static TcpClient client;
        private static StreamReader sr;
        private static StreamWriter sw;
        private static bool connected = false;
        private static readonly object connLock = new object();

        private static readonly Random rnd = new Random();

        static void Main()
        {
            Console.WriteLine($"[WAVY‑TEMP] Inicializar ({WAVY_ID})");

            // Cria e inicia as threads para envio de dados e watchdog
            new Thread(SendLoop) { IsBackground = true }.Start();
            new Thread(WatchdogLoop) { IsBackground = true }.Start();

            Console.WriteLine("[WAVY‑TEMP] A correr — prima Enter para terminar.");
            Console.ReadLine();
        }

        
        /// Loop responsável por enviar dados ao AGREGADOR de forma periódica.
        /// Ação: envia temperatura simulada em JSON a cada intervalo definido.
        
        private static void SendLoop()
        {
            while (true)
            {
                Thread.Sleep(SEND_INTERVAL);

                lock (connLock)
                {
                    if (!connected) continue;

                    try
                    {
                        // Geração de valor de temperatura aleatório
                        double temp = Math.Round(15 + rnd.NextDouble() * 10, 2);
                        var payload = new
                        {
                            temperature_c = temp,
                            timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
                        };
                        string json = JsonSerializer.Serialize(payload);

                        // Envio da mensagem com os dados
                        string msg = $"ENVIAR_DADOS;ID={WAVY_ID};JSON={json}";
                        sw.WriteLine(msg);
                        Console.WriteLine("[WAVY‑TEMP] => " + msg);

                        // Aguarda resposta do AGREGADOR
                        string resp = sr.ReadLine();
                        if (resp == null)
                            throw new IOException("Agregador fechou a ligação.");
                        Console.WriteLine("[WAVY‑TEMP] <= " + resp);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("[WAVY‑TEMP] Erro envio: " + ex.Message);
                        TearDown();
                    }
                }
            }
        }

        
        /// Loop que verifica o estado da ligação com o AGREGADOR (heartbeat).
        /// Ação: envia "PING" e aguarda "PONG"; se falhar, tenta reconectar.
        
        private static void WatchdogLoop()
        {
            while (true)
            {
                Thread.Sleep(WATCHDOG_MS);

                lock (connLock)
                {
                    if (!connected)
                    {
                        TryConnect();
                    }
                    else
                    {
                        try
                        {
                            sw.WriteLine("PING");
                            string pong = sr.ReadLine();
                            if (pong == null || !pong.Contains("PONG"))
                                throw new IOException("Heartbeat falhou");
                        }
                        catch (Exception)
                        {
                            Console.WriteLine("[WAVY‑TEMP] Heartbeat falhou — desligar.");
                            TearDown();
                        }
                    }
                }
            }
        }

        
        /// Tenta estabelecer ligação com o AGREGADOR.
        /// Ação: cria socket TCP, envia pedido de conexão com ID e estado.
        /// Resultado: atualiza o estado da ligação (connected = true/false).
        
        private static void TryConnect()
        {
            try { client?.Close(); } catch { }

            try
            {
                client = new TcpClient();
                client.Connect(IPAddress.Parse(AGG_IP), AGG_PORT);

                var ns = client.GetStream();
                sr = new StreamReader(ns);
                sw = new StreamWriter(ns) { AutoFlush = true };

                string hello = $"PEDIDO_CONEXAO;ID={WAVY_ID};STATE={WAVY_STATE}";
                sw.WriteLine(hello);
                string resp = sr.ReadLine();

                connected = resp != null && resp.Contains("OK_CONEXAO");
                Console.WriteLine(connected
                    ? "[WAVY‑TEMP] Ligado ao Agregador!"
                    : "[WAVY‑TEMP] Handshake rejeitado: " + resp);
            }
            catch (Exception ex)
            {
                Console.WriteLine("[WAVY‑TEMP] Conexão falhou: " + ex.Message);
                connected = false;
            }
        }

        
        /// Finaliza a ligação, fecha o socket e atualiza o estado da conexão.
        
        private static void TearDown()
        {
            try { client?.Close(); } catch { }
            connected = false;
        }
    }
}
