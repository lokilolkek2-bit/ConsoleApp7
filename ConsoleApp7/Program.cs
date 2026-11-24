using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;

class TcpServer
{
    private const int Port = 5000;
    private static readonly HttpClient httpClient = new();

    // Активные подключения и задачи их обработки.
    private static readonly ConcurrentDictionary<int, TcpClient> clients = new();
    private static readonly ConcurrentDictionary<int, Task> clientTasks = new();
    private static int nextClientId;

    static async Task Main()
    {
        var listener = new TcpListener(IPAddress.Any, Port);
        using var serverCts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            Console.WriteLine("\n[Сервер] Получена команда остановки...");
            serverCts.Cancel();
        };

        listener.Start();
        Console.WriteLine($"[Сервер] Запущен на порту {Port}.");
        Console.WriteLine("[Сервер] Ожидание подключений. Для остановки нажмите Ctrl+C.");

        try
        {
            while (!serverCts.Token.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    // Ожидание нового клиента не блокирует обработку уже подключённых.
                    client = await listener.AcceptTcpClientAsync(serverCts.Token);
                }
                catch (OperationCanceledException) when (serverCts.IsCancellationRequested)
                {
                    break;
                }
                catch (ObjectDisposedException) when (serverCts.IsCancellationRequested)
                {
                    break;
                }

                int id = Interlocked.Increment(ref nextClientId);
                clients[id] = client;
                Console.WriteLine($"[Сервер] Клиент #{id} подключён: {client.Client.RemoteEndPoint}");

                // Обслуживание клиента запускается отдельно. Цикл приёма подключений
                // сразу продолжает работу и может принять следующего клиента.
                Task clientTask = HandleClientAsync(client, id, serverCts.Token);
                clientTasks[id] = clientTask;
            }
        }
        catch (SocketException ex) when (serverCts.IsCancellationRequested)
        {
            Console.WriteLine($"[Сервер] Сокет закрывается: {ex.Message}");
        }
        finally
        {
            listener.Stop();

            // Закрытие сокетов разблокирует операции чтения активных клиентов.
            foreach (TcpClient client in clients.Values)
                client.Close();

            try
            {
                await Task.WhenAll(clientTasks.Values);
            }
            catch (Exception ex)
            {
                // Ошибка отдельного подключения не должна скрывать завершение сервера.
                Console.WriteLine($"[Сервер] Ошибка при завершении клиентских задач: {ex.Message}");
            }

            httpClient.Dispose();
            Console.WriteLine("[Сервер] Работа завершена.");
        }
    }

    private static async Task HandleClientAsync(TcpClient client, int id, CancellationToken serverToken)
    {
        try
        {
            using (client)
            using (NetworkStream stream = client.GetStream())
            {
                byte[] buffer = new byte[1024];

                while (!serverToken.IsCancellationRequested)
                {
                    int bytesRead = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), serverToken);

                    // ReadAsync возвращает 0, когда клиент корректно закрыл соединение.
                    if (bytesRead == 0)
                    {
                        Console.WriteLine($"[Сервер] Клиент #{id} отключился.");
                        break;
                    }

                    string received = Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();
                    Console.WriteLine($"[Клиент #{id}] Получено: \"{received}\"");

                    string response;
                    if (received.StartsWith("PING ", StringComparison.OrdinalIgnoreCase))
                    {
                        string url = received[5..].Trim();
                        response = await PingSiteAsync(url, serverToken);
                    }
                    else
                    {
                        response = received.ToUpperInvariant();
                    }

                    byte[] data = Encoding.UTF8.GetBytes(response);
                    await stream.WriteAsync(data.AsMemory(0, data.Length), serverToken);
                    Console.WriteLine($"[Сервер] Клиенту #{id} отправлено: \"{response}\"");
                }
            }
        }
        catch (OperationCanceledException) when (serverToken.IsCancellationRequested)
        {
            Console.WriteLine($"[Сервер] Обработка клиента #{id} остановлена.");
        }
        catch (IOException ex)
        {
            Console.WriteLine($"[Сервер] Соединение с клиентом #{id} потеряно: {ex.Message}");
        }
        catch (SocketException ex)
        {
            Console.WriteLine($"[Сервер] Сетевая ошибка клиента #{id}: {ex.Message}");
        }
        catch (ObjectDisposedException)
        {
            Console.WriteLine($"[Сервер] Соединение клиента #{id} закрыто.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Сервер] Ошибка клиента #{id}: {ex.Message}");
        }
        finally
        {
            clients.TryRemove(id, out _);
            client.Close();
            Console.WriteLine($"[Сервер] Клиент #{id} удалён из списка подключений.");
        }
    }

    private static async Task<string> PingSiteAsync(string url, CancellationToken serverToken)
    {
        if (string.IsNullOrWhiteSpace(url))
            return "ERROR: URL не указан";

        if (!url.StartsWith("http://", StringComparison.OrdinalIgnoreCase) &&
            !url.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            url = "https://" + url;
        }

        var stopwatch = Stopwatch.StartNew();
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(serverToken);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(5));

        try
        {
            using var response = await httpClient.GetAsync(
                url, HttpCompletionOption.ResponseHeadersRead, timeoutCts.Token);
            stopwatch.Stop();
            return $"PONG: {url} — {stopwatch.ElapsedMilliseconds} ms (HTTP {(int)response.StatusCode})";
        }
        catch (OperationCanceledException) when (serverToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            stopwatch.Stop();
            return $"ERROR: Таймаут ({stopwatch.ElapsedMilliseconds} ms) для {url}";
        }
        catch (HttpRequestException ex)
        {
            stopwatch.Stop();
            return $"ERROR: {ex.Message}";
        }
    }
}
