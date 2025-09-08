from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
import base64

def generate_keys(key_size=2048):
    """Gera um par de chaves RSA."""
    key = RSA.generate(key_size)
    private_key = key
    public_key = key.publickey()
    return private_key, public_key

def sign(private_key, data):
    """Assina um dado com a chave privada."""
    if isinstance(data, str):
        data = data.encode('utf-8')
    h = SHA256.new(data)
    signature = pkcs1_15.new(private_key).sign(h)
    return base64.b64encode(signature).decode('utf-8')

def verify_signature(public_key, data, signature):
    """Verifica a assinatura de um dado com a chave pública."""
    if isinstance(data, str):
        data = data.encode('utf-8')
    try:
        signature_bytes = base64.b64decode(signature)
        h = SHA256.new(data)
        pkcs1_15.new(public_key).verify(h, signature_bytes)
        return True
    except (ValueError, TypeError):
        return False

def get_public_key_pem(public_key):
    """Retorna a chave pública no formato PEM."""
    return public_key.export_key()

def load_public_key_from_pem(pem_data):
    """Carrega uma chave pública a partir do formato PEM."""
    return RSA.import_key(pem_data)