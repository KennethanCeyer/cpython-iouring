from _asynciouring import IOUring


def io_uring_open(path):
    """
    Open a file using io_uring.

    Args:
        path (str): The file path to open.

    Returns:
        IOUring: An IOUring object with read() and print() methods.
    """
    return IOUring(path)
