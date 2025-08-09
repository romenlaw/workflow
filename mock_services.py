from typing import List
import random
import time
import inspect
from mcp.client.sse import sse_client
from mcp import ClientSession

from workflow_mgmt import workstep_defn, LinearRetryPolicy, get_current_context, _execute_workstep_wrapper

class IdService:
    mapping = {
        "fid": "gen_fid",
        "oid": "gen_oid",
        "tid": "gen_tid"
    }
    MCP_URL = "http://127.0.0.1:8000/sse"

    @workstep_defn(step_id="GenMeMoId", bian_sd="MAF",
                   retry_policy=LinearRetryPolicy(max_retries=3, base_delay=1.0, exclude_exceptions=[ValueError, RuntimeError])
                   )
    @staticmethod
    async def gen_id(type:str)->str:
        """generate a MeMo ID
        Args:
            type (str): type of id to generate. Valid values are 'fid', 'oid', 'tid'
        Returns:
            id (str): the generated ID 
        """
        tool_name = IdService.mapping[type]
        async with sse_client(url=IdService.MCP_URL) as streams:
            async with ClientSession(*streams) as session:
                await session.initialize()
                response = await session.call_tool(tool_name)
                if response.isError:
                    print(f"\t💥 Non-retryable error in IdService.gen_id {response}")
                    raise RuntimeError(response.content[0].text)
                
                id = response.content[0].text
                return id
            
    @workstep_defn(step_id="GenTids", bian_sd="MAF",
                   retry_policy=LinearRetryPolicy(max_retries=3, base_delay=1.0, exclude_exceptions=[ValueError, RuntimeError])
                   )
    @staticmethod
    async def gen_tids(quantity:int)->List[str]:
        """generate a MeMo ID
        Args:
            quantity (int): number of TIDs to generate. valid range is 1 to 100 (inclusive).
        Returns:
            ids (List[str]): the list of generated TIDs 
        """
        async with sse_client(url=IdService.MCP_URL) as streams:
            ids = []
            async with ClientSession(*streams) as session:
                await session.initialize()
                response = await session.call_tool("gen_tids",  arguments={"quantity": quantity})
                if response.isError:
                    print(f"\t💥 Non-retryable error in IdService.gen_tids {response}")
                    raise RuntimeError(response.content[0].text)
                
                ids = [content.text for content in response.content]
                return ids
            
class MafService:
    @workstep_defn(step_id="MafOnboarding", bian_sd="MAF")
    @staticmethod
    def onboard_merchant(payload:str):
        print(f"\tonboarding new merchant: {payload}")

        # simulate error conditions
        if random.random() < .1: # 10% chance of connection error
            raise ConnectionError("\t💣 Failed to connect to external API")
        
        if random.random() < .2: # 20% chance of timeout
            time.sleep(3)
            raise TimeoutError("\t💣 Took too long.")

        if random.random() < .1: # 10% chance of data value error
            print(f"\t💥 Non-retryable error in MafService.onboard_merchant")
            raise ValueError("Wrong merchant data, go fix it.")
        
        print("\tMafService successful!")
        return "✅ MafService successful!"
    
class CpsdService:
    @workstep_defn(step_id="LinkCustomerProduct", bian_sd="CPSD",)
    @staticmethod
    def link_customer_product(payload:str):
        print(f"\tlinking customer and product: {payload}")

        # simulate error conditions
        if random.random() < .1: # 10% chance of connection error
            raise ConnectionError("\t💣 Failed to connect to external API")
        
        if random.random() < .2: # 20% chance of timeout
            time.sleep(3)
            raise TimeoutError("\t💣 Took too long.")

        if random.random() < .1: # 10% chance of data value error
            print(f"💥 Non-retryable error in CpsdService.link_customer_product")
            raise ValueError("Wrong customer or product data, go fix it.")
        
        print("\tCpsdService successful!")
        return "✅ CpsdService successful!"
    
class IpbService:
    @workstep_defn(step_id="CreatIpbParties", bian_sd="IPB")
    @staticmethod
    def onboard_merchant(payload:str):
        print(f"\tcreating parties and hierarchy in IPB: {payload}")

        # simulate error conditions
        if random.random() < .1: # 10% chance of connection error
            raise ConnectionError("💣 Failed to connect to external API")
        
        if random.random() < .2: # 20% chance of timeout
            time.sleep(3)
            raise TimeoutError("💣 Took too long.")

        if random.random() < .1: # 10% chance of data value error
            print(f"💥 Non-retryable error in IpbService.onboard_merchant")
            raise ValueError("Wrong customer or merchant data, go fix it.")
        
        print("\tIpbService merchant onboarding successful!")
        return "✅ IpbService merchant onboarding successful!"
    
    @workstep_defn(step_id="LinkNegotiatedPricing", bian_sd="IPB")
    @staticmethod
    def link_pricing(payload):
        raise NotImplemented("💥 creating of negotiated pricing and linking to merchant have not yet been implemented")
    
class CtaService:
    @workstep_defn(step_id="CtaOnboarding", bian_sd="CTA")
    @staticmethod
    def onboard_merchant(payload:str):
        print(f"\tcreating parties and hierarchy in TMS: {payload}")

        # simulate error conditions
        if random.random() < .1: # 10% chance of connection error
            raise ConnectionError("💣 Failed to connect to external API")
        
        if random.random() < .2: # 20% chance of timeout
            time.sleep(3)
            raise TimeoutError("💣 Took too long.")

        if random.random() < .1: # 10% chance of data value error
            print(f"💥 Non-retryable error in CtaService.onboard_merchant")
            raise ValueError("Wrong merchant data, go fix it.")
        
        print("\tCtaService merchant onboarding successful!")
        return "✅ CpaService merchant onboarding successful!"
