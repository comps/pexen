class PexenWarning(UserWarning):
    pass

class PexenError(Exception):
    pass

class DepcheckError(PexenError):
    """Raised by the scheduler when a requires/provides check fails.

    The arg contains the remaining unsatisfied dependency metadata.
    """
    def __init__(self, msg, remain):
        self.remain = remain
        super().__init__(msg)
