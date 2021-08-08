def dispatcher(command, params):
    if command == '/':
        return 'Hello there'
    elif command == '/test':
        return 'Passed'
    else:
        raise KeyError
