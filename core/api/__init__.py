from .base_api import BaseAPI
from .moysklad import MoyskladAPI

__all__ = ['BaseAPI', 'MoyskladAPI']


def check_connection(session):
    return None