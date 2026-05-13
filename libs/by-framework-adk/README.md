# by-framework-adk

ADK (Agent Development Kit) integration for `by-framework`.

Provides `AdkWorker` to easily create Byai workers using Google ADK agents.

## Usage

```python
from google.adk.agents.llm_agent import LlmAgent
from by_framework_adk.worker import AdkWorker

class MyAdkWorker(AdkWorker):
    def get_agent_types(self):
        return ["my-adk-agent"]
        
    def build_agent(self, context, command) -> LlmAgent:
        return LlmAgent(
            name="my_agent",
            model="gemini-2.0-flash",
            instruction="You are a helpful assistant.",
        )
```
