using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;

namespace ServerApp
{
    // Representa o envelope de dados recebido do AGREGADOR
    class Envelope
    {
        public string wavyId { get; set; }        // ID da WAVY de origem
        public JsonElement[] batch { get; set; }  // Conjunto de dados (JSON)
    }

    class Program
    {
        private static readonly object fileLock = new();  // Proteção de acesso aos ficheiros

        
        // Função principal. Inicia o servidor na porta definida e aceita conexões dos AGREGADORES.
        
        static void Main(string[] args)
        {
            int port = 6000;
            Console.WriteLine($"[SERVIDOR] Iniciando na porta {port}...");

            var listener = new TcpListener(IPAddress.Any, port);
            listener.Start();

            while (true)
            {
                Console.WriteLine("[SERVIDOR] Aguardando conexões de Agregadores...");
                TcpClient aggClient = listener.AcceptTcpClient(); // Aguarda conexão
                Console.WriteLine("[SERVIDOR] Agregador conectado!");
                new Thread(() => HandleAggregator(aggClient)).Start(); // Nova thread por ligação
            }
        }

        
        // Gere a comunicação com um AGREGADOR.
        // Ação: lê as mensagens recebidas, processa e responde.
        
        static void HandleAggregator(TcpClient aggClient)
        {
            try
            {
                using var ns = aggClient.GetStream();
                using var sr = new StreamReader(ns);
                using var sw = new StreamWriter(ns) { AutoFlush = true };

                bool connected = false;
                string aggregatorId = "AGG-UNKNOWN";

                while (true)
                {
                    string line = sr.ReadLine();
                    if (line == null)
                    {
                        Console.WriteLine("[SERVIDOR] Conexão fechada pelo Agregador.");
                        break;
                    }

                    if (!line.Equals("PING", StringComparison.Ordinal))
                        Console.WriteLine("[SERVIDOR] Recebido => " + line);

                    string response = ProcessMessage(line, ref connected, ref aggregatorId);
                    sw.WriteLine(response);

                    if (!response.Equals("PONG", StringComparison.Ordinal))
                        Console.WriteLine("[SERVIDOR] Resposta => " + response);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("[SERVIDOR] Erro => " + ex.Message);
            }
            finally
            {
                aggClient.Close();
            }
        }

        
        // Processa uma mensagem recebida de um AGREGADOR.
        // Ação: valida o tipo de mensagem, executa ação e devolve resposta.
        
        static string ProcessMessage(string msg, ref bool connected, ref string aggregatorId)
        {
            if (msg == "PING")
            {
                return "PONG";
            }
            else if (msg.StartsWith("PEDIDO_CONEXAO"))
            {
                connected = true;
                var parts = msg.Split(';');
                if (parts.Length > 1 && parts[1].StartsWith("AGG="))
                    aggregatorId = parts[1].Substring("AGG=".Length);
                return "OK_CONEXAO";
            }
            else if (msg.StartsWith("ENVIAR_DADOS") && connected)
            {
                int idx = msg.IndexOf("JSON=", StringComparison.Ordinal);
                if (idx < 0) return "ERRO_FORMATO_ENVIAR_DADOS";

                string jsonEnvelope = msg.Substring(idx + 5);

                try
                {
                    var envelope = JsonSerializer.Deserialize<Envelope>(jsonEnvelope);
                    if (envelope?.wavyId == null || envelope.batch == null)
                        return "ERRO_FORMATO_JSON";

                    SaveDataToFile(envelope);
                    return "PODE_APAGAR_CACHE";
                }
                catch (JsonException)
                {
                    return "ERRO_PARSE_JSON";
                }
            }
            else if (msg.StartsWith("END_CONN"))
            {
                return "ACK_END_CONN";
            }
            else
            {
                return !connected ? "ERRO_NAO_CONECTADO" : "ERRO_COMANDO_DESCONHECIDO";
            }
        }

        
        // Grava os dados recebidos no sistema de ficheiros.
        // Parâmetros: envelope – dados com ID da WAVY e lote de leituras.
        // Ação: escreve cada linha com timestamp e dados JSON num ficheiro CSV.
       
        static void SaveDataToFile(Envelope envelope)
        {
            string dir = Path.Combine("server_data", envelope.wavyId);
            Directory.CreateDirectory(dir);

            string fileName = Path.Combine(dir, DateTime.Now.ToString("yyyy-MM-dd") + ".csv");

            lock (fileLock) // Garante que só uma thread escreve de cada vez
            {
                using var writer = File.AppendText(fileName);
                foreach (var record in envelope.batch)
                {
                    string line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss};{record.GetRawText()}";
                    writer.WriteLine(line);
                }
            }
        }
    }
}
