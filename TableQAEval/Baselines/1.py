# Load model directly
from transformers import AutoTokenizer, AutoModelForCausalLM

tokenizer = AutoTokenizer.from_pretrained("Salesforce/xgen-7b-8k-inst")
model = AutoModelForCausalLM.from_pretrained("Salesforce/xgen-7b-8k-inst")