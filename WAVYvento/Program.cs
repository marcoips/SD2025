using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;

namespace WavyWindApp
{
    class Program
    {
        // Parâmetros de configuração da WAVY
        private const string AGG_IP = "127.0.0.1";            // Endereço IP do Agregador
        private const int AGG_PORT = 5000;                    // Porto TCP de comunicação
        private const string WAVY_ID = "WAVYWIND001";         // ID da WAVY
        private const string WAVY_STATE = "operacao";         // Estado inicial
        private static readonly TimeSpan SEND_INTERVAL = TimeSpan.FromSeconds(20); // Intervalo de envio de dados
        private const int WATCHDOG_MS = 5000;                 // Intervalo de verificação da ligação

        // Estado da conexão
        private static TcpClient client;
        private static StreamReader sr;
        private static StreamWriter sw;
        private static bool connected = false;
        private static readonly object connLock = new object();

        private static readonly Random rnd = new Random();

        
        /// Função principal da aplicação. Inicia as threads de envio e watchdog.
        
        static void Main()
        {
            Console.WriteLine($"[WAVY‑WIND] Inicializar (ID={WAVY_ID})");

            new Thread(SendLoop) { IsBackground = true }.Start();     // Thread de envio de dados
            new Thread(WatchdogLoop) { IsBackground = true }.Start(); // Thread de verificação da ligação

            Console.WriteLine("[WAVY‑WIND] A correr — Enter para sair.");
            Console.ReadLine(); // Mantém a aplicação ativa até o utilizador pressionar Enter
        }

        
        /// Envia dados de velocidade do vento em ciclos definidos.
        /// Parâmetros: nenhum.
        /// Ação: Envia JSON com velocidade do vento e timestamp para o AGREGADOR.
        
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
                        double wind = Math.Round(rnd.NextDouble() * 80, 1); // Geração de valor de vento entre 0–80 km/h
                        var payload = new
                        {
                            wind_speed_kmh = wind,
                            timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
                        };
                        string json = JsonSerializer.Serialize(payload);

                        string msg = $"ENVIAR_DADOS;ID={WAVY_ID};JSON={json}";
                        sw.WriteLine(msg);
                        Console.WriteLine("[WAVY‑WIND] => " + msg);

                        string resp = sr.ReadLine();
                        if (resp == null) throw new IOException("Agregador fechou a ligação.");
                        Console.WriteLine("[WAVY‑WIND] <= " + resp);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("[WAVY‑WIND] Erro envio: " + ex.Message);
                        TearDown();
                    }
                }
            }
        }

        
        /// Verifica periodicamente se a ligação com o AGREGADOR está ativa.
        /// Parâmetros: nenhum.
        /// Ação: Envia "PING" e espera "PONG". Se falhar, marca a ligação como inativa.
        
        private static void WatchdogLoop()
        {
            while (true)
            {
                Thread.Sleep(WATCHDOG_MS);

                lock (connLock)
                {
                    if (!connected)
                    {
                        TryConnect(); // Tenta ligar-se ao AGREGADOR
                    }
                    else
                    {
                        try
                        {
                            sw.WriteLine("PING");
                            string pong = sr.ReadLine();
                            if (pong == null || !pong.Contains("PONG"))
                                throw new IOException("Heartbeat falhou.");
                        }
                        catch (Exception)
                        {
                            Console.WriteLine("[WAVY‑WIND] Heartbeat falhou — desligado.");
                            TearDown();
                        }
                    }
                }
            }
        }

        
        /// Tenta estabelecer ligação com o AGREGADOR.
        /// Parâmetros: nenhum.
        /// Ação: Estabelece a ligação TCP e realiza handshake com envio de ID e estado.
        
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
                    ? "[WAVY‑WIND] Ligado ao Agregador!"
                    : "[WAVY‑WIND] Handshake rejeitado: " + resp);
            }
            catch (Exception ex)
            {
                Console.WriteLine("[WAVY‑WIND] Ligação falhou: " + ex.Message);
                connected = false;
            }
        }

        
        /// Encerra a ligação com o AGREGADOR e atualiza o estado.
        
        private static void TearDown()
        {
            try { client?.Close(); } catch { }
            connected = false;
        }
    }
}
