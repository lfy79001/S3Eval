import torch
from transformers import LlamaTokenizer, LlamaForCausalLM



tokenizer = LlamaTokenizer.from_pretrained('meta-llama/Llama-2-7b-chat-hf', trust_remote_code=True)
model = LlamaForCausalLM.from_pretrained('meta-llama/Llama-2-7b-chat-hf', trust_remote_code=True, torch_dtype=torch.bfloat16)