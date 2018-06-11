from riberry import model


def add_user(username, password, first_name, last_name, display_name, department, email):
    user = model.auth.User(
        username=username,
        password=model.auth.User.secure_password(password).decode(),
        auth_provider='default',
        details=model.auth.UserDetails(
            first_name=first_name,
            last_name=last_name,
            display_name=display_name,
            department=department,
            email=email
        )
    )
    model.conn.add(user)
    model.conn.commit()

    return user.id
