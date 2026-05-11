"""
Ejecutar una sola vez para crear el usuario admin inicial.
Uso: python crear_admin.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import SessionLocal
from app.models import Usuario
from app.services.auth import hash_password

def main():
    nombre   = input("Nombre del admin: ").strip()
    email    = input("Email: ").strip().lower()
    password = input("Contraseña: ").strip()

    if not nombre or not email or not password:
        print("ERROR: todos los campos son obligatorios")
        return

    db = SessionLocal()
    try:
        existente = db.query(Usuario).filter(Usuario.email == email).first()
        if existente:
            print(f"Ya existe un usuario con el email {email}")
            return

        admin = Usuario(
            nombre   = nombre,
            email    = email,
            password = hash_password(password),
            rol      = "admin",
            contratos= None,
            activo   = True,
        )
        db.add(admin)
        db.commit()
        print(f"✓ Admin '{nombre}' creado con email {email}")
    finally:
        db.close()

if __name__ == "__main__":
    main()
