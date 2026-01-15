from openai import OpenAI
import json

client = OpenAI(
  base_url="https://integrate.api.nvidia.com/v1",
  
)

completion = client.chat.completions.create(
  model="qwen/qwen3-next-80b-a3b-instruct",
  messages=[{"role":"user","content":"time complx of kmp algo"}],
  temperature=0.6,
  top_p=0.7,
  max_tokens=4096,
  stream=True
)


for chunk in completion:
  if chunk.choices[0].delta.content:
    print(chunk.choices[0].delta.content, end="")

