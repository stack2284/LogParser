import torch
import torch.nn as nn

class LogSequenceGRU(nn.Module):
    def __init__(self, vocab_size, embed_dim=32, hidden_dim=64):
        super(LogSequenceGRU, self).__init__()
        self.embedding = nn.Embedding(vocab_size, embed_dim)
        # Use GRU instead of LSTM for learning sequence patterns [cite: 127-129]
        self.gru = nn.GRU(embed_dim, hidden_dim, batch_first=True) 
        self.fc = nn.Linear(hidden_dim, vocab_size)

    def forward(self, x):
        embedded = self.embedding(x)
        out, _ = self.gru(embedded)
        return self.fc(out[:, -1, :])