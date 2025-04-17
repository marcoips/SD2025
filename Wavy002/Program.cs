using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;

namespace WavyRainApp
{
    class Program
    {
        // Configurações iniciais
        private const string AGG_IP = "127.0.0.1";               // IP do AGREGADOR
        private const int AGG_PORT = 5000;                       // Porto de ligação
        private const string WAVY_ID = "WAVYRAIN001";            // ID da WAVY
        private const string WAVY_STATE = "operacao";            // Estado inicial

        private static readonly TimeSpan SEND_INTERVAL = TimeSpan.FromSeconds(20); // Intervalo de envio
        private static readonly int WATCHDOG_MS = 5000;                             // Intervalo do watchdog

        private static TcpClient client;
        private static StreamReader sr;
        private static StreamWriter sw;
        private static bool connected = false;
        private static readonly object connLock = new object();

        private static readonly Random rnd = new Random();

        
        /// Ponto de entrada da aplicação WAVY.
        /// Ação: inicia as threads de envio e watchdog.
        
        static void Main()
        {
            Console.WriteLine($"[WAVY‑RAIN] Iniciar (ID={WAVY_ID})");

            var sendThread = new Thread(SendLoop) { IsBackground = true };
            sendThread.Start();

            var watchdogThread = new Thread(WatchdogLoop) { IsBackground = true };
            watchdogThread.Start();

            Console.ReadLine(); // Mantém o processo em execução
        }

        
        /// Envia dados de percentagem de chuva periodicamente.
        /// Ação: simula leitura, cria JSON e envia para o AGREGADOR.
        
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
                        double rain = Math.Round(rnd.NextDouble() * 100, 1); // 0–100%
                        var payload = new
                        {
                            rain_percent = rain,
                            timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
                        };
                        string json = JsonSerializer.Serialize(payload);

                        string msg = $"ENVIAR_DADOS;ID={WAVY_ID};JSON={json}";
                        sw.WriteLine(msg);
                        Console.WriteLine("[WAVY‑RAIN] => " + msg);

                        string resp = sr.ReadLine();
                        if (resp == null) throw new IOException("Agregador fechou a conexão.");
                        Console.WriteLine("[WAVY‑RAIN] <= " + resp);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("[WAVY‑RAIN] Falha ao enviar: " + ex.Message);
                        TearDown();
                    }
                }
            }
        }

        
        /// Verifica periodicamente se a ligação com o AGREGADOR está ativa.
        /// Ação: envia "PING" e espera "PONG". Se falhar, reinicia a ligação.
        
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
                            string resp = sr.ReadLine();
                            if (resp == null || !resp.Contains("PONG"))
                                throw new IOException("Heartbeat falhou.");
                        }
                        catch (Exception)
                        {
                            Console.WriteLine("[WAVY‑RAIN] Heartbeat falhou — desligado.");
                            TearDown();
                        }
                    }
                }
            }
        }

        
        /// Tenta ligar-se ao AGREGADOR.
        /// Ação: estabelece a ligação e envia pedido de conexão com ID e estado.
        
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

                if (resp != null && resp.Contains("OK_CONEXAO"))
                {
                    connected = true;
                    Console.WriteLine("[WAVY‑RAIN] Ligado ao Agregador!");
                }
                else
                {
                    Console.WriteLine("[WAVY‑RAIN] Handshake falhou: " + resp);
                    TearDown();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("[WAVY‑RAIN] Conexão falhou: " + ex.Message);
                TearDown();
            }
        }

        
        /// Encerra a ligação e marca o estado como desligado.
        
        private static void TearDown()
        {
            try { client?.Close(); } catch { }
            connected = false;
        }
    }
}
