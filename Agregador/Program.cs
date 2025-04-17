
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;

namespace AggregatorApp
{
    // Representa uma entrada no ficheiro wavy_config.csv
    class WavyConfig
    {
        public string WavyId;
        public string Status;       // Estado atual: associada, operacao, manutencao, desativada
        public string DataTypes;    // Tipos de dados que envia (ex: "[temp]")
        public string LastSync;     // Última sincronização (timestamp)
    }

    // Representa uma entrada no ficheiro preprocess_config.csv
    class PreProcessInfo
    {
        public string WavyId;
        public string PreProcess;       // Tipo de pré-processamento (raw, average, etc.)
        public int VolumeDadosEnviar;   // Volume mínimo de dados antes do envio
        public string ServidorAssociado;// IP:Porto do servidor associado
    }

    class Program
    {
        // Caminhos dos ficheiros de configuração
        private const string WAVY_CONFIG_FILE = "wavy_config.csv";
        private const string PREPROCESS_CONFIG_FILE = "preprocess_config.csv";

        // Endereço do servidor por defeito
        private const string DEFAULT_SERVER_IP = "127.0.0.1";
        private const int DEFAULT_SERVER_PORT = 6000;

        private const int LISTEN_PORT = 5000; // Porta onde o AGREGADOR escuta as WAVYs

        private static readonly TimeSpan MAX_FLUSH_INTERVAL = TimeSpan.FromMinutes(5); // Tempo máximo antes de flush
        private const int HEARTBEAT_MS = 10_000; // Intervalo de verificação do servidor

        // Estruturas de configuração em memória
        private static readonly List<WavyConfig> wavyConfigList = new();
        private static readonly List<PreProcessInfo> preProcessList = new();
        private static readonly object configLock = new(); // Lock para proteger acesso a configuração

        // Buffers de dados e tempos de último envio
        private static readonly Dictionary<string, List<string>> batchBuffers = new();
        private static readonly Dictionary<string, DateTime> lastFlush = new();
        private static readonly object batchLock = new(); // Lock para proteger buffers

        // Ligação ao servidor
        private static TcpClient serverClient;
        private static StreamReader srServer;
        private static StreamWriter swServer;
        private static bool serverConnected = false;
        private static readonly object serverLock = new();

        
        // Função principal. Inicia o carregamento das configurações, ligação ao servidor,
        // heartbeat e escuta de WAVYs.
        
        static void Main()
        {
            LoadWavyConfig(WAVY_CONFIG_FILE);
            LoadPreProcessConfig(PREPROCESS_CONFIG_FILE);

            ConnectToServer();
            new Thread(HeartbeatLoop) { IsBackground = true }.Start();

            var listener = new TcpListener(IPAddress.Any, LISTEN_PORT);
            listener.Start();
            Console.WriteLine($"[AGG] A escutar na porta {LISTEN_PORT}...");

            while (true)
            {
                var sock = listener.AcceptTcpClient();
                Console.WriteLine("[AGG] WAVY conectada.");
                new Thread(() => HandleWavy(sock)).Start(); // Cria nova thread para cada WAVY
            }
        }

        
        // Carrega o ficheiro wavy_config.csv para memória.
        // Formato: WAVY_ID:status:[data_types]:last_sync
        
        static void LoadWavyConfig(string filename)
        {
            lock (configLock)
            {
                wavyConfigList.Clear();
                if (!File.Exists(filename)) return;
                foreach (var line in File.ReadAllLines(filename))
                {
                    var p = line.Split(':');
                    if (p.Length < 4) continue;
                    wavyConfigList.Add(new WavyConfig
                    {
                        WavyId = p[0],
                        Status = p[1],
                        DataTypes = p[2],
                        LastSync = p[3]
                    });
                }
            }
        }

        
        // Carrega o ficheiro preprocess_config.csv para memória.
        // Formato: WAVY_ID:pré_processamento:volume_dados_enviar:servidor_associado
        
        static void LoadPreProcessConfig(string filename)
        {
            lock (configLock)
            {
                preProcessList.Clear();
                if (!File.Exists(filename)) return;
                foreach (var line in File.ReadAllLines(filename))
                {
                    var p = line.Split(':');
                    if (p.Length < 4) continue;
                    preProcessList.Add(new PreProcessInfo
                    {
                        WavyId = p[0],
                        PreProcess = p[1],
                        VolumeDadosEnviar = int.Parse(p[2]),
                        ServidorAssociado = p[3]
                    });
                }
            }
        }

        
        // Atualiza o estado de uma WAVY e grava no ficheiro.
        
        static void UpdateWavyStatus(string wavyId, string newStatus)
        {
            lock (configLock)
            {
                var w = wavyConfigList.Find(x => x.WavyId == wavyId) ?? new WavyConfig { WavyId = wavyId };
                w.Status = newStatus;
                if (!wavyConfigList.Contains(w)) wavyConfigList.Add(w);
                SaveWavyConfig();
            }
        }

        
        // Atualiza a data da última sincronização da WAVY.
        
        static void UpdateWavyLastSync(string wavyId, string syncTime)
        {
            lock (configLock)
            {
                var w = wavyConfigList.Find(x => x.WavyId == wavyId);
                if (w != null)
                {
                    w.LastSync = syncTime;
                    SaveWavyConfig();
                }
            }
        }

        
        // Grava as alterações ao ficheiro wavy_config.csv.
        
        static void SaveWavyConfig()
        {
            var lines = new List<string>();
            foreach (var w in wavyConfigList)
                lines.Add($"{w.WavyId}:{w.Status}:{w.DataTypes}:{w.LastSync}");
            File.WriteAllLines(WAVY_CONFIG_FILE, lines);
        }

        
        // Procura as definições de pré-processamento para uma WAVY.
        
        static PreProcessInfo FindPreProcess(string wavyId)
        {
            lock (configLock)
                return preProcessList.Find(p => p.WavyId == wavyId);
        }

        
        // Tenta estabelecer ou restabelecer a ligação com o servidor.
        
        static void ConnectToServer()
        {
            lock (serverLock)
            {
                if (serverConnected) return;
                try { serverClient?.Close(); } catch { }

                try
                {
                    serverClient = new TcpClient();
                    serverClient.Connect(DEFAULT_SERVER_IP, DEFAULT_SERVER_PORT);
                    var ns = serverClient.GetStream();
                    srServer = new StreamReader(ns);
                    swServer = new StreamWriter(ns) { AutoFlush = true };

                    swServer.WriteLine("PEDIDO_CONEXAO;AGG=AGG1");
                    var resp = srServer.ReadLine();
                    serverConnected = resp != null && resp.Contains("OK_CONEXAO");
                    Console.WriteLine(serverConnected
                        ? "[AGG] Conectado ao Servidor."
                        : "[AGG] Handshake falhou no Servidor.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("[AGG] Erro conectar Servidor: " + ex.Message);
                    serverConnected = false;
                }
            }
        }

        
        // Envia "PING" ao servidor e aguarda "PONG" para garantir que está ativo.
        
        static void HeartbeatLoop()
        {
            while (true)
            {
                Thread.Sleep(HEARTBEAT_MS);
                lock (serverLock)
                {
                    if (!serverConnected)
                    {
                        ConnectToServer();
                    }
                    else
                    {
                        try
                        {
                            swServer.WriteLine("PING");
                            var pong = srServer.ReadLine();
                            if (pong == null || !pong.Contains("PONG"))
                                throw new IOException("PONG inválido");
                        }
                        catch
                        {
                            Console.WriteLine("[AGG] Heartbeat falhou — reconectando.");
                            serverConnected = false;
                            try { serverClient.Close(); } catch { }
                        }
                    }
                }
            }
        }

        
        // Trata a ligação com uma WAVY.
        // Ação: lê mensagens recebidas e responde conforme o protocolo.
        
        static void HandleWavy(TcpClient sock)
        {
            bool connected = false;
            string wavyId = null;

            try
            {
                using var ns = sock.GetStream();
                using var sr = new StreamReader(ns);
                using var sw = new StreamWriter(ns) { AutoFlush = true };

                while (true)
                {
                    var msg = sr.ReadLine();
                    if (msg == null) break;

                    Console.WriteLine($"[AGG] <- {msg}");
                    var response = ProcessWavyMessage(msg, ref connected, ref wavyId);
                    sw.WriteLine(response);
                    Console.WriteLine($"[AGG] -> {response}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AGG] Erro WAVY ({wavyId}): {ex.Message}");
            }
            finally
            {
                sock.Close();
            }
        }

        
        // Processa uma mensagem recebida da WAVY.
        // Ação: interpreta comandos como PEDIDO_CONEXAO e ENVIAR_DADOS.
        // Parâmetros: msg – mensagem recebida; connected/wavyId – estados atualizados.
        // Resultado: resposta de acordo com o protocolo.
        
        static string ProcessWavyMessage(string msg, ref bool connected, ref string wavyId)
        {
            // Tratamento do handshake inicial
            if (msg.StartsWith("PEDIDO_CONEXAO"))
            {
                var parts = msg.Split(';');
                if (parts.Length < 2) return "ERRO_FORMATO_CONEXAO";

                wavyId = parts[1].Split('=')[1];
                connected = true;
                var state = parts.Length > 2 ? parts[2].Split('=')[1] : "associada";
                UpdateWavyStatus(wavyId, state);

                // Inicializa buffers caso seja a primeira vez
                lock (batchLock)
                {
                    if (!batchBuffers.ContainsKey(wavyId))
                    {
                        batchBuffers[wavyId] = new();
                        lastFlush[wavyId] = DateTime.Now;
                    }
                }
                return "OK_CONEXAO";
            }

            // Tratamento do envio de dados
            if (msg.StartsWith("ENVIAR_DADOS") && connected)
            {
                var parts = msg.Split(';');
                if (parts.Length < 3) return "ERRO_FORMATO_ENVIAR_DADOS";

                var id = parts[1].Split('=')[1];
                var jsonPart = parts[2].Substring(5); // após "JSON="

                UpdateWavyLastSync(id, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                SaveLocal(id, jsonPart, DateTime.Now); // Gravação local imediata

                // Adição ao buffer
                bool flush = false;
                lock (batchLock)
                {
                    if (!batchBuffers.ContainsKey(id))
                    {
                        batchBuffers[id] = new();
                        lastFlush[id] = DateTime.Now;
                    }

                    batchBuffers[id].Add(jsonPart);

                    // Verifica se deve fazer flush (volume ou tempo)
                    var pinfo = FindPreProcess(id);
                    var vol = pinfo?.VolumeDadosEnviar ?? batchBuffers[id].Count;

                    if (batchBuffers[id].Count >= vol
                        || DateTime.Now - lastFlush[id] >= MAX_FLUSH_INTERVAL)
                    {
                        flush = true;
                    }
                }

                if (!flush)
                    return "PODE_APAGAR_CACHE";

                // Prepara e envia lote de dados
                List<string> lote;
                lock (batchLock)
                {
                    lote = new(batchBuffers[id]);
                    batchBuffers[id].Clear();
                    lastFlush[id] = DateTime.Now;
                }

                var envelope = JsonSerializer.Serialize(new
                {
                    wavyId = id,
                    batch = lote
                });

                bool ok = SendToServer(envelope);
                return ok ? "PODE_APAGAR_CACHE" : "ERRO_SERVIDOR";
            }

            // Encerramento de ligação
            if (msg.StartsWith("END_CONN"))
                return "ACK_END_CONN";

            return !connected
                ? "ERRO_NAO_CONECTADO"
                : "ERRO_COMANDO_DESCONHECIDO";
        }

        
        // Grava localmente os dados recebidos da WAVY em ficheiros csv organizados por data.
        
        static void SaveLocal(string wavyId, string json, DateTime stamp)
        {
            var dir = Path.Combine("data", wavyId);
            Directory.CreateDirectory(dir);
            var file = Path.Combine(dir, stamp.ToString("yyyy-MM-dd") + ".csv");
            File.AppendAllText(file, json + Environment.NewLine);
        }

        
        // Envia o lote de dados em formato JSON ao SErv.
        // Parâmetros: envelopeJson – lote de dados.
        // Resultado: true se o envio for confirmado, false se falhar.
        
        static bool SendToServer(string envelopeJson)
        {
            lock (serverLock)
            {
                if (!serverConnected) return false;

                try
                {
                    swServer.WriteLine($"ENVIAR_DADOS;JSON={envelopeJson}");
                    var resp = srServer.ReadLine();
                    return resp != null && resp.Contains("PODE_APAGAR_CACHE");
                }
                catch
                {
                    serverConnected = false;
                    try { serverClient.Close(); } catch { }
                    return false;
                }
            }
        }
    }
}