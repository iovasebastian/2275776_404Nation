from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class AutomationRule(Base):
    __tablename__ = "rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sensor_name = Column(String, nullable=False)
    operator = Column(String, nullable=False)
    threshold_value = Column(Float, nullable=False)
    actuator_name = Column(String, nullable=False)
    action_state = Column(String, nullable=False)

# mapping of operator strings to actual comparison functions
OPERATORS = {
    '<': lambda a, b: a < b,
    '<=': lambda a, b: a <= b,
    '=': lambda a, b: a == b,
    '>': lambda a, b: a > b,
    '>=': lambda a, b: a >= b
}

def create_session(database_url: str):
    engine = create_engine(database_url)
    Base.metadata.create_all(bind=engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)

def evaluate_rule(rule: AutomationRule, current_value: float) -> bool:
    op = OPERATORS.get(rule.operator)
    if not op:
        return False
    return op(current_value, rule.threshold_value)

def get_all_rules(db_session):
    return db_session.query(AutomationRule).all()

def list_rules_for_sensor(db_session, sensor_name: str):
    return db_session.query(AutomationRule).filter(AutomationRule.sensor_name == sensor_name).all()
