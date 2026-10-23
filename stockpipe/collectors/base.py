import logging
from abc import ABC, abstractmethod
from datetime import date

from sqlalchemy.orm import Session

from stockpipe.config import Settings

class BaseCollector(ABC):
	def __init__(self, settings: Settings, session: Session) -> None:
		self.settings = settings
		self.db = session
		self.logger = logging.getLogger(self.__class__.__name__)

	@abstractmethod
	def collect(self, target_date: date | None = None) -> int:
		...
	@abstractmethod
	def validate(self, data: list[dict]) -> list[dict]:
		...