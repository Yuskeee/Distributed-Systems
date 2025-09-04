from Crypto.PublicKey import RSA
import os

def generate_keys_in_folder(num_keys, folder_name="keys"):
    """
    Generates a specified number of RSA key pairs and saves them to a folder.

    Args:
        num_keys (int): The number of key pairs to generate.
        folder_name (str): The name of the folder to store the keys in. Defaults to "keys".
    """

    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    for i in range(num_keys):
        key = RSA.generate(2048)

        private_key = key.export_key()
        private_key_filename = os.path.join(folder_name, f"private_{i}.pem")
        with open(private_key_filename, "wb") as f:
            f.write(private_key)
        print(f"Private key saved to: {private_key_filename}")

        public_key = key.publickey().export_key()
        public_key_filename = os.path.join(folder_name, f"public_{i}.pem")
        with open(public_key_filename, "wb") as f:
            f.write(public_key)
        print(f"Public key saved to: {public_key_filename}")

generate_keys_in_folder(2, "keys")