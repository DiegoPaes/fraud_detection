import sys
import logging

def error_message_detail(error, error_detail: sys):
    """
    Obtém informações detalhadas sobre o erro ocorrido.

    Parameters:
    error: Exception - A exceção capturada.
    error_detail: sys - O módulo sys para obter os detalhes do erro.

    Returns:
    str - Mensagem detalhada sobre o erro, incluindo o nome do arquivo e o número da linha.
    """
    _, _, exc_tb = error_detail.exc_info()
    file_name = exc_tb.tb_frame.f_code.co_filename
    error_message = (
        f"Erro ocorreu no script python [{file_name}] "
        f"número da linha [{exc_tb.tb_lineno}] "
        f"mensagem de erro [{error}]"
    )
    return error_message


class CustomException(Exception):
    def __init__(self, error_message, error_detail: sys):
        """
        Inicializa a exceção customizada.

        Parameters:
        error_message: str - A mensagem de erro.
        error_detail: sys - O módulo sys para obter os detalhes do erro.
        """
        super().__init__(error_message)
        self.error_message = error_message_detail(error_message, error_detail)

    def __str__(self):
        """
        Retorna a mensagem de erro detalhada.

        Returns:
        str - A mensagem de erro formatada.
        """
        return self.error_message