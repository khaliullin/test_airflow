# from cryptography.fernet import Fernet
# from cryptography.hazmat.backends import _get_backend
# import os
# import base64
#
# class Do(object):
#     def __init__(self, key: bytes, backend=None):
#         backend = _get_backend(backend)
#
#         key = base64.urlsafe_b64decode(key)
#         if len(key) != 32:
#             raise ValueError(
#                 "Fernet key must be 32 url-safe base64-encoded bytes."
#             )
#
#         self._signing_key = key[:16]
#         self._encryption_key = key[16:]
#         self._backend = backend
#
#     @classmethod
#     def generate_key(cls) -> bytes:
#         # os.environ['AWAY_SECRET_KEY']
#         return base64.urlsafe_b64encode(str.encode(os.environ['AWAY_SECRET_KEY']))
#
# def generate_key():
#     """
#     Generates a key and save it into a file
#     """
#     # key = Fernet.generate_key()
#     key = Do.generate_key()
#     with open("away-secret.key", "wb") as key_file:
#         key_file.write(key)
#
# def load_key():
#     """
#     Load the previously generated key
#     """
#     return open("away-secret.key", "rb").read()
#
# def encrypt_message(message):
#     """
#     Encrypts a message
#     """
#     key = load_key()
#     encoded_message = message.encode()
#     f = Fernet(key)
#     encrypted_message = f.encrypt(encoded_message)
#     return encrypted_message
#
# def decrypt_message(encrypted_message):
#     """
#     Decrypts an encrypted message
#     """
#     key = load_key()
#     f = Fernet(key)
#     decrypted_message = f.decrypt(encrypted_message)
#     return decrypted_message.decode()
#
# def doDecrypt(encrypted_message):
#     generate_key()
#     return decrypt_message(str.encode(encrypted_message))
#
# def doEncrypt(message):
#     generate_key()
#     return encrypt_message(message)
#
#
# if __name__ == "__main__":
#     generate_key()
#     message = encrypt_message(os.environ['AWAY_SECRET_KEY'])
#     print(message)
#     # print(decrypt_message(message))
#     s = decrypt_message(message)
#     if os.environ['AWAY_SECRET_KEY'] == s:
#         print("Success decrypted:\n" + s)
#     else:
#         print("Fail decrypted")
#
#
#     message = "gAAAAABhjEFo81GkNIZG3Idmz5F0fgJ2pGdQ-iKc6vaJ-8NZ6Nrmfa7LMpMLL659fll7wJGe7ltaPwJaMUkEJ4jVLFlPtz91huOhRMhjfn0EKVQSnaxig2c0dNoGcHPw9pJ494XGqXqU"
#     s = doDecrypt(message)
#     print(s)