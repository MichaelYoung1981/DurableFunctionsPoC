using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace SFA.DAS.DurableFunction.POC
{
    public static class PaymentOrchestrator
    {
        private const string ConnectionString = "Server=tcp:durablefunctionpoc.database.windows.net,1433;Initial Catalog=DurableFunctionPOC;Persist Security Info=False;User ID=MickYoung1981;Password=AdminPassword123;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";

        [FunctionName("PaymentOrchestrator")]
        public static async Task RunOrchestrator([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            log.LogInformation($"Started orchestrator with ID {context.InstanceId}");
            var retryPolicy = new RetryOptions(new TimeSpan(0,0,0,1), 10);
            retryPolicy.BackoffCoefficient = 2;

            var learners = await context.CallActivityAsync<List<Learner>>("GetLearners", null);
            if (learners.Count > 0)
            {
                log.LogInformation($"{learners.Count} learners to be processed");
                var learnerTasks = new List<Task>();
                foreach (var learner in learners)
                {
                    log.LogInformation($"Creating request to calculate payments for ULN: {learner.Uln} - LegalEntity {learner.LegalEntityId}");
                    Task task = context.CallActivityWithRetryAsync("CalculatePaymentsForLearner", retryPolicy, learner);
                    learnerTasks.Add(task);
                }

                log.LogInformation($"{learnerTasks.Count} tasks created");
                await Task.WhenAll(learnerTasks);

                context.ContinueAsNew(null);
                return;
            }

            log.LogInformation("All payment calculations complete");

            var parallelTasks = new List<Task>();

            var legalEntities = await context.CallActivityAsync<List<long>>("GetLegalEntities", null);
            foreach (var legalEntity in legalEntities)
            {
                Task task = context.CallActivityWithRetryAsync("PayLegalEntity", retryPolicy, legalEntity);
                parallelTasks.Add(task);
            }

            await Task.WhenAll(parallelTasks);
        }

        [FunctionName("GetLearners")]
        public static async Task<List<Learner>> GetLearners([ActivityTrigger] string name, ILogger log)
        {
            List<Learner> learners;

            using (var connection = new SqlConnection(ConnectionString))
            {
                await connection.OpenAsync();
                learners = (await connection.QueryAsync<Learner>("SELECT TOP 10000 Uln, LegalEntityId FROM PendingPayment WHERE IsDue = 1 AND PaymentCalculated = 0")).ToList();
                log.LogInformation($"{learners.Count} learners selected");
                connection.Close();
            }

            return learners;
        }

        [FunctionName("GetLegalEntities")]
        public static async Task<List<long>> GetLegalEntities([ActivityTrigger] string name, ILogger log)
        {
            List<long> legalEntities;

            using (var connection = new SqlConnection(ConnectionString))
            {
                await connection.OpenAsync();
                legalEntities = (await connection.QueryAsync<long>("SELECT DISTINCT LegalEntityId FROM Payment WHERE HasBeenPaid = 0")).ToList();
                log.LogInformation($"{legalEntities.Count} legalEntities selected");
                connection.Close();
            }

            return legalEntities;
        }

        [FunctionName("CalculatePaymentsForLearner")]
        public static async Task CalculatePaymentsForLearner([ActivityTrigger] Learner learner, ILogger log)
        {
            log.Log(LogLevel.Information, $"Creating payments for uln {learner.Uln} - legal entity {learner.LegalEntityId}");
            int paymentCount = 0;
            using (var connection = new SqlConnection(ConnectionString))
            {
                await connection.OpenAsync();
                var pendingPayments = await connection.QueryAsync<PendingPayment>("SELECT Id, Uln, LegalEntityId, Amount FROM PendingPayment WHERE IsDue = 1 AND PaymentCalculated = 0 AND Uln = @uln AND LegalEntityId = @legalEntityId", new { learner.Uln, learner.LegalEntityId });

                
                foreach(var pendingPayment in pendingPayments)
                { 
                    await connection.ExecuteAsync("INSERT INTO Payment (Id, Uln, LegalEntityId, Amount, HasBeenPaid) VALUES (NEWID(), @uln, @legalEntityId, @amount, 0)", new {pendingPayment.Uln, pendingPayment.LegalEntityId, pendingPayment.Amount});
                    await connection.ExecuteAsync("UPDATE PendingPayment SET PaymentCalculated = 1 WHERE Id = @Id", new { pendingPayment.Id });
                    paymentCount++;
                }

                connection.Close();
            }
            log.Log(LogLevel.Information, $"{paymentCount} payments created for uln {learner.Uln} - legal entity {learner.LegalEntityId}");
        }

        [FunctionName("PayLegalEntity")]
        public static async Task PayLegalEntity([ActivityTrigger] long legalEntityId, ILogger log)
        {
            using (var connection = new SqlConnection(ConnectionString)
            )
            {
                await connection.OpenAsync();
                var paymentAmount = await connection.ExecuteScalarAsync<decimal>("SELECT SUM(Amount) FROM Payment WHERE LegalEntityId = @legalEntityId AND HasBeenPaid = 0", new { LegalEntityId = legalEntityId });

                await connection.ExecuteAsync("INSERT INTO SummarisedPayment (Id, LegalEntityId, Amount, PaymentDate) VALUES (NEWID(), @legalEntityId, @amount, GETDATE())", new { LegalEntityId = legalEntityId, Amount = paymentAmount });
                await connection.ExecuteAsync("UPDATE Payment SET HasBeenPaid = 1 WHERE LegalEntityId = @legalEntityId AND HasBeenPaid = 0", new { LegalEntityId = legalEntityId });
                connection.Close();
            }
        }

        [FunctionName("PaymentOrchestrator_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "orchestrators/{functionName}/{instanceId}")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            string functionName,
            string instanceId,
            ILogger log)
        {

            var existingInstance = await starter.GetStatusAsync(instanceId);
            if (existingInstance == null)
            {
                await starter.StartNewAsync(functionName, instanceId);

                log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

                return starter.CreateCheckStatusResponse(req, instanceId);
            }

            return new HttpResponseMessage(HttpStatusCode.Conflict)
            {
                Content = new StringContent($"An instance with ID '{instanceId}' already exists."),
            };
        }

        public class Learner
        {
            public long Uln { get; set; }
            public long LegalEntityId { get; set; }
        }

        public class PendingPayment
        {
            public Guid Id { get; set; }
            public long Uln { get; set; }
            public long LegalEntityId { get; set; }
            public decimal Amount { get; set; }
        }
    }
}