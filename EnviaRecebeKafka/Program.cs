using Confluent.Kafka;
using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

namespace EnviaRecebeKafka
{
    class Program
    {
        public static async Task Main()
        {
            //Configura o log
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.File("logs/EnviaRecebeKafka.txt", rollingInterval: RollingInterval.Day)
                .CreateLogger();


            // dispara uma nova thread para envio de mensagens
            Thread envia = new Thread(EnviaKafka);
            Log.Information("Thread de envio iniciada");
            envia.Start();

            // dispara uma nova thread para recebimento de mensagens
            Thread recebe = new Thread(RecebeKafka);
            Log.Information("Thread de recebimento iniciada");
            recebe.Start();


        }
        public static async void EnviaKafka()
        {
            var config = new ProducerConfig { BootstrapServers = "127.0.0.1:9092" };

            //Pega o id do microserviso, que será usado para identificar o topico Kafka para envio.
            string idMricroserviceProdutor = ConfigurationManager.AppSettings["idMicroServiceProdutor"];
            Log.Debug($"Enviando com o Id: {idMricroserviceProdutor}");
            using var p = new ProducerBuilder<Null, string>(config).Build();
            {
                try
                {
                    while (true)
                    {
                        var idRequisicao = Guid.NewGuid().ToString();
                        string mensagem = $"Hello World! Enviador por: {idMricroserviceProdutor}, Em: {DateTime.Now} ,  Id:  { idRequisicao } ";

                        //Envia uma mensagem para o topico do id que vai consumir.
                        var dr = await p.ProduceAsync(idMricroserviceProdutor,
                            new Message<Null, string> { Value = mensagem });

                        Log.Debug($"Mensagem enviada: {mensagem}");

                        //Aguara para enviar a proxima msg.
                        Thread.Sleep(5000);
                    }
                }
                catch (Exception e)
                {
                    Log.Error(e, "Erro no envio");
                }
            }
        }

        public static void RecebeKafka()
        {
            //Pega o id do microserviso, que será usado para identificar o topico Kafka para recebimento.
            string idMricroserviceConsumidor = ConfigurationManager.AppSettings["idMicroServiceConsumer"];

            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "127.0.0.1:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var c = new ConsumerBuilder<Ignore, string>(conf).Build();
            {
                //Conecta no topico que está enviando mensagens.
                c.Subscribe(idMricroserviceConsumidor);
                Log.Debug($"Recebendo do Id: {idMricroserviceConsumidor}");
                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine(cr.Value);

                            Log.Debug($"Mensagem recebida: {cr.Value}");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
    }
}
