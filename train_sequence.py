import torch
import torch.nn as nn
import torch.optim as optim
from sequence_model import LogSequenceGRU

# Simulated parsed cluster sequence (e.g., mapped from C0001 -> 1)
data = [1, 2, 1, 3, 1, 2, 1, 3, 1, 4]
vocab_size = 5
seq_length = 3

def create_sequences(data, seq_len):
    xs, ys = [], []
    for i in range(len(data) - seq_len):
        xs.append(data[i:i+seq_len])
        ys.append(data[i+seq_len])
    return torch.tensor(xs), torch.tensor(ys)

X, y = create_sequences(data, seq_length)

model = LogSequenceGRU(vocab_size)
criterion = nn.CrossEntropyLoss()
optimizer = optim.Adam(model.parameters(), lr=0.01)

for epoch in range(50):
    optimizer.zero_grad()
    predictions = model(X)
    loss = criterion(predictions, y)
    loss.backward()
    optimizer.step()
    
    if (epoch+1) % 10 == 0:
        print(f'Epoch {epoch+1}, Loss: {loss.item():.4f}')

test_seq = torch.tensor([[1, 2, 1]])
actual_next = 4 # Inject anomaly

model.eval()
with torch.no_grad():
    pred = model(test_seq)
    pred_id = torch.argmax(pred, dim=1).item()
    
    if pred_id != actual_next:
        print(f"Sequence Anomaly! Expected {pred_id}, got {actual_next}")
    else:
        print("Normal sequence.")