package com.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.OrchestrationRunner;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;
import java.util.*;

/**
 * Azure Functions with HTTP Trigger.
 */
public class DurableFunction
{
    /**
     * This HTTP-triggered function starts the orchestration.
     */
    @FunctionName("StartOrchestration")
    public HttpResponseMessage startOrchestration(
            @HttpTrigger(
                name = "req", 
                methods = {HttpMethod.GET, HttpMethod.POST}, 
                authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) 
    {
        
        DurableTaskClient client = durableContext.getClient();
        String instanceId;

        // Parse query parameter
        String query = request.getQueryParameters().get("type");

        if (query!= null && query.equals("FOFI"))
        {
            context.getLogger().info("Java HTTP trigger processed a durable FOFI request.");
            instanceId = client.scheduleNewOrchestrationInstance("FanOutFanIn");
        }
        else
        {
            context.getLogger().info("Java HTTP trigger processed a durable chaining request.");
            instanceId = client.scheduleNewOrchestrationInstance("Chain");
        }
                
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);

        return durableContext.createCheckStatusResponse(request, instanceId);
    }


    /**
     * This is the orchestrator function. The OrchestrationRunner.loadAndRun() static
     * method is used to take the function input and execute the orchestrator logic.
     */
    @FunctionName("Chain")
    public String citiesOrchestrator(
        @DurableOrchestrationTrigger(name = "orchestratorRequestProtoBytes") String orchestratorRequestProtoBytes) {
        return OrchestrationRunner.loadAndRun(orchestratorRequestProtoBytes, ctx -> {
            String result = "";
            result += ctx.callActivity("Capitalize", "Tokyo", String.class).await() + ", ";
            result += ctx.callActivity("Capitalize", "London", String.class).await() + ", ";
            result += ctx.callActivity("Capitalize", "Seattle", String.class).await() + ", ";
            result += ctx.callActivity("Capitalize", "Austin", String.class).await();
            return result;
        });
    }
        
    @FunctionName("FanOutFanIn")
    public String fanOutFanInOrchestrator(
            @DurableOrchestrationTrigger(name = "runtimeState") String runtimeState) {
        return OrchestrationRunner.loadAndRun(runtimeState, ctx -> {
            
            // 3^4 + 3^3 + 3^2 + 3^1
            int base = 3;
            int exp  = 4;

            //List<Task<Integer>> valList = new ArrayList<Task<Integer>>();
            List <Integer> valList = new ArrayList<Integer>();
            for (int i = exp; i>=1; i--)
            {
                int intArr[] = new int[2];
                intArr[0] = base;
                intArr[1] = i;
    
                Integer val = ctx.callActivity("Calculation", intArr, Integer.class).await();
                valList.add(val);
            }

            int result = valList.stream().mapToInt(Integer::intValue).sum();
            
            return result;
        });
    }    

    /**
     * This is the activity function that gets invoked by the orchestration.
     */
    @FunctionName("Capitalize")
    public String capitalize(
        @DurableActivityTrigger(name = "name") String name,
        final ExecutionContext context) 
    {
        context.getLogger().info("Capitalizing: " + name);

        return name.toUpperCase();
    }

    @FunctionName("Calculation")
    public Integer calculation(
        @DurableActivityTrigger(name = "input") int[] intArr,
        final ExecutionContext context) 
    {                
        context.getLogger().info("Calculating: " + intArr[0] + "^" + intArr[1]);
        int returnVal = (int) Math.pow(intArr[0],intArr[1]);
        context.getLogger().info("returnVal: " + returnVal);
        
        return Integer.valueOf(returnVal);
    }

}
