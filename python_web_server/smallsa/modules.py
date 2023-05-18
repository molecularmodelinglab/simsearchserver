import math
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.nn import TransformerDecoderLayer as DecLayer
from torch.nn import TransformerDecoder as Decoder
from torch.nn import TransformerEncoderLayer as EncLayer
from torch.nn import TransformerEncoder as Encoder


def get_downstream_mask(seq):
    _, ls = seq.size()
    dvc = seq.device
    mask = torch.triu(torch.ones((ls, ls), device=dvc), diagonal=1).bool()
    return mask


def count_params(model):
    return sum([p.nelement() for p in model.parameters()])


class PositionalEncoding(nn.Module):
    def __init__(self, d_model, dropout=0.1, max_len=200):  # 200):
        super().__init__()
        self.dropout = nn.Dropout(p=dropout)

        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, d_model, 2).float() \
                             * (-math.log(10000.0) / d_model))
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0)
        self.register_buffer('pe', pe)

    def forward(self, x):
        x = x + self.pe[:, :x.size(1)]
        return self.dropout(x)


class PropMLP(nn.Module):
    def __init__(self, n_targets, dim_hidden):
        super().__init__()
        self.fc_layers = nn.Sequential(
            nn.Linear(dim_hidden, 256),
            nn.ReLU(inplace=True),
            nn.Linear(256, n_targets),
        )

    def forward(self, x):
        x = self.fc_layers(x)
        return x


class SeqAutoencoder(nn.Module):
    def __init__(self,
                 n_tokens=39,
                 max_len=152,  # 135, #122,
                 dim_emb=512,
                 heads=8,
                 dim_hidden=32,
                 L_enc=6,
                 L_dec=6,
                 dim_ff=2048,
                 drpt=0.1,
                 actv='relu',
                 eps=0.6,
                 b_first=True,
                 num_props=None,
                 **kwargs):
        """

        Args:
            dim_emb: dimensions of input embedding
            heads: number of attention heads
            dim_hidden: dimensions of latent space
            L_enc: number of layers in encoder
            L_dec: number of layers in decoder
            dim_ff: dimensions of feed forward network
        """
        super().__init__()

        self.n_tokens = n_tokens
        self.max_len = max_len
        self.dim_hidden = dim_hidden
        self.dim_emb = dim_emb

        # Initial embedding and subsequent positional encoder
        self.embedder = nn.Embedding(self.n_tokens, dim_emb)
        self.pos_enc = PositionalEncoding(dim_emb, dropout=drpt)

        # Encoder
        enc_layer = EncLayer(d_model=dim_emb, nhead=heads,
                             dim_feedforward=dim_ff, dropout=drpt,
                             activation=actv, layer_norm_eps=eps,
                             batch_first=b_first)
        self.enc = Encoder(enc_layer, num_layers=L_enc)

        # Up-sample
        self.linear = nn.Linear(dim_emb, dim_hidden)
        self.samp_linear = nn.Linear(dim_hidden, dim_emb * max_len)

        # Decoder
        dec_layer = DecLayer(d_model=dim_emb, nhead=heads,
                             dim_feedforward=dim_ff, dropout=drpt,
                             activation=actv, layer_norm_eps=eps,
                             batch_first=b_first)
        self.dec = Decoder(dec_layer, num_layers=L_dec)
        self.decode_out = nn.Linear(dim_emb, self.n_tokens)

        self.prop_nn = None
        if num_props is not None:
            self.prop_nn = PropMLP(num_props, dim_hidden)

    def forward(self, seq, pad_mask=None, avg_mask=None, out_mask=None,
                normed=True, bottleneck=True, labels=None):

        if len(seq.shape) == 1:
            seq = seq.unsqueeze(0)

        # Masks
        mask = get_downstream_mask(seq)  # casual mask ...
        dec_mask = mask
        mem_mask = mask
        if avg_mask is None:
            avg_mask = torch.ones_like(seq)
        if out_mask is None:
            out_mask = torch.zeros(self.n_tokens).to(seq.device)

        # Encode
        emb_seq = self.pos_enc(self.embedder(seq))
        enc_out = self.enc(src=emb_seq, mask=mask, src_key_padding_mask=pad_mask)
        # out -> (bs, 120, 512)

        # Situate the latent vector
        if bottleneck:
            enc_sum = (avg_mask.unsqueeze(2) * enc_out).sum(axis=1)
            enc_avg = enc_sum / (avg_mask.sum(axis=1).unsqueeze(1))
            # out -> (bs, 512)
            latent_vec = self.linear(enc_avg)
            if normed:
                latent_vec = F.normalize(latent_vec, p=2.0, dim=-1)
            latent_out = self.samp_linear(latent_vec)
            latent_out = latent_out.reshape(-1, self.max_len, self.dim_emb)
        else:
            latent_out = enc_out

        # Decode
        dec_out = self.dec(tgt=emb_seq, memory=latent_out,
                           tgt_mask=dec_mask, memory_mask=mem_mask,
                           tgt_key_padding_mask=pad_mask,
                           memory_key_padding_mask=pad_mask)
        dec_out = self.decode_out(dec_out).masked_fill(out_mask == 1, -1e9)

        prop_pred = None
        if self.prop_nn is not None:
            lat = torch.tensor(latent_vec).to(seq.device).float()
            prop_pred = self.prop_nn(lat)

        return latent_vec, dec_out, prop_pred

    def latent(self, seq, pad_mask=None, avg_mask=None, out_mask=None,
                normed=True, bottleneck=True, labels=None):

        if len(seq.shape) == 1:
            seq = seq.unsqueeze(0)

        # Masks
        mask = get_downstream_mask(seq)  # casual mask ...
        dec_mask = mask
        mem_mask = mask
        if avg_mask is None:
            avg_mask = torch.ones_like(seq)
        if out_mask is None:
            out_mask = torch.zeros(self.n_tokens).to(seq.device)

        # Encode
        emb_seq = self.pos_enc(self.embedder(seq))
        enc_out = self.enc(src=emb_seq, mask=mask, src_key_padding_mask=pad_mask)
        # out -> (bs, 120, 512)

        # Situate the latent vector
        if bottleneck:
            enc_sum = (avg_mask.unsqueeze(2) * enc_out).sum(axis=1)
            enc_avg = enc_sum / (avg_mask.sum(axis=1).unsqueeze(1))
            # out -> (bs, 512)
            latent_vec = self.linear(enc_avg)
            if normed:
                latent_vec = F.normalize(latent_vec, p=2.0, dim=-1)
        return latent_vec

    def generate(self, start_idx, end_idx, pad_idx, batch_size=1,
                 ltnt_code=None, greedy=False, use_cuda=None,
                 use_out_mask=True):

        if use_cuda is None:
            use_cuda = torch.cuda.is_available()
        device = torch.device("cuda" if use_cuda else "cpu")

        self.to(device)
        self.eval()

        if ltnt_code is None:
            ltnt_code = torch.randn((batch_size,
                                     self.dim_hidden)).to(device)
            ltnt_code = F.normalize(ltnt_code, p=2.0, dim=-1)

        if ltnt_code.dim() == 1:
            ltnt_code = ltnt_code.unsqueeze(0).repeat([batch_size, 1])

        if ltnt_code.dim() == 2:
            ltnt_code = self.samp_linear(ltnt_code)
            ltnt_code = ltnt_code.reshape(-1, self.max_len, self.dim_emb)

        batch_size = ltnt_code.shape[0]
        max_len = ltnt_code.shape[1]
        ltnt_code = ltnt_code.to(device)

        out_mask = torch.ones(self.n_tokens)
        idx_tens = torch.tensor([start_idx, pad_idx])
        out_mask = torch.zeros_like(out_mask).scatter_(0,
                                                       idx_tens,
                                                       out_mask).to(device)

        if not use_out_mask:
            out_mask = torch.zeros_like(out_mask)

        seq = torch.tensor([start_idx for _ in range(batch_size)]).unsqueeze(1).to(device)

        causal_mask = get_downstream_mask(ltnt_code[..., 0])

        with torch.no_grad():
            for p in range(max_len):
                dec_mask = causal_mask[:seq.shape[-1], :seq.shape[-1]]
                mem_mask = causal_mask[:seq.shape[-1]]
                #                 mem_mask = None

                emb_seq = self.pos_enc(self.embedder(seq))
                model_out = self.dec(emb_seq, ltnt_code,
                                     tgt_mask=dec_mask,
                                     memory_mask=mem_mask)

                model_out = self.decode_out(model_out)

                logits = model_out.masked_fill(out_mask == 1, -1e9)

                if greedy:
                    top_i = torch.argmax(logits[:, -1], dim=-1)
                else:
                    top_i = torch.distributions.categorical.Categorical(
                        logits=logits[:, -1]).sample()

                top_i = top_i.masked_fill((seq[:, -1] == end_idx) | (seq[:, -1] == pad_idx), pad_idx)

                seq = torch.cat([seq, top_i.unsqueeze(1)], dim=-1)

            close_seq = torch.tensor([end_idx for _ in range(batch_size)]).to(device)
            close_seq = close_seq.masked_fill((seq[:, -1] == end_idx) | (seq[:, -1] == pad_idx), pad_idx)

            seq = torch.cat([seq, close_seq.unsqueeze(1)], dim=-1)

        return seq
