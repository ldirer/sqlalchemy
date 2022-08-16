from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import orm
from sqlalchemy import Select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import ORMExecuteState
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker


class Context:
    def __init__(self):
        self.current_account = None

    def get_current_account(self):
        return self.current_account

    def set_current_account(self, account):
        self.current_account = account


context = Context()


@event.listens_for(Session, "do_orm_execute")
def _add_filtering_criteria(execute_state: ORMExecuteState):
    """Intercept all ORM queries.   Add a with_loader_criteria option to all
    of them.

    This adds a filter by account id to provide automatic 'tenant isolation'.
    """

    # the with_loader_criteria automatically applies itself to
    # relationship loads as well including lazy loads.   So if this is
    # a relationship load, assume the option was set up from the top level
    # query.

    if (
        not execute_state.is_relationship_load
        and not execute_state.is_column_load
        and not execute_state.execution_options.get(
            "include_all_accounts", False
        )
        # avoid infinite recursion where getting the account makes a request
        # that needs to get the account and...
        and not (
            isinstance(execute_state.statement, Select)
            and len(execute_state.statement.get_final_froms()) == 1
            and isinstance(execute_state.statement.get_final_froms()[0], Table)
            and execute_state.statement.get_final_froms()[0].name
            == Account.__tablename__
        )
    ):
        account = context.get_current_account()
        current_account_id = account.id
        if current_account_id is not None:
            execute_state.statement = execute_state.statement.options(
                orm.with_loader_criteria(
                    AccountBoundMixin,
                    lambda cls: cls.account_id == current_account_id,
                    include_aliases=True,
                    track_closure_variables=True,
                )
            )


Base = declarative_base()


class Account(Base):
    __tablename__ = "account"

    id = Column(
        String,
        primary_key=True,
    )
    name = Column(String)


class AccountBoundMixin:
    @declared_attr
    def account_id(cls):
        return Column(
            String, ForeignKey("account.id"), index=True, nullable=False
        )

    @declared_attr
    def account(cls):
        return relationship("Account")


class User(Base, AccountBoundMixin):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    addresses = relationship("Address", back_populates="user")


class Address(Base, AccountBoundMixin):
    __tablename__ = "address"

    id = Column(Integer, primary_key=True)
    email = Column(String)
    user_id = Column(Integer, ForeignKey("user.id"))

    user = relationship("User", back_populates="addresses")


if __name__ == "__main__":
    MAKE_TESTS_PASS = False

    kwargs = dict() if not MAKE_TESTS_PASS else dict(query_cache_size=0)

    engine = create_engine("sqlite://", pool_pre_ping=True, **kwargs)

    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, future=True)

    sess = Session()

    acc1 = Account(name="test account 1", id="acc1")
    acc2 = Account(name="test account 2", id="acc2")
    sess.add_all([acc1, acc2])
    sess.flush()

    context.set_current_account(account=acc1)

    sess.add_all(
        [
            User(
                name="u1",
                account_id=acc1.id,
                addresses=[
                    Address(email="u1a1", account_id=acc1.id),
                    Address(email="u1a2", account_id=acc1.id),
                ],
            ),
            User(
                name="u2",
                account_id=acc1.id,
                addresses=[
                    Address(email="u2a1", account_id=acc2.id),
                    Address(email="u2a2", account_id=acc1.id),
                ],
            ),
            User(
                name="u3",
                account_id=acc2.id,
                addresses=[
                    Address(email="u3a1", account_id=acc2.id),
                    Address(email="u3a2", account_id=acc2.id),
                ],
            ),
            User(
                name="u4",
                account_id=acc2.id,
                addresses=[
                    Address(email="u4a1", account_id=acc2.id),
                    Address(email="u4a2", account_id=acc1.id),
                ],
            ),
            User(
                name="u5",
                account_id=acc1.id,
                addresses=[
                    Address(email="u5a1", account_id=acc1.id),
                    Address(email="u5a2", account_id=acc2.id),
                ],
            ),
        ]
    )

    sess.commit()

    # now querying Address or User objects only gives us the ones from the
    # current account
    for u1 in sess.query(User).options(orm.selectinload(User.addresses)):
        assert u1.account_id == acc1.id

        # the addresses collection will also be "current account only",
        # which works for all relationship loaders including joinedload
        for address in u1.addresses:
            assert address.account_id == acc1.id

    # change account and check we only get data from the 'new current account'
    context.set_current_account(account=acc2)
    for u1 in sess.query(User).options(orm.selectinload(User.addresses)):
        assert u1.account_id == acc2.id

        for address in u1.addresses:
            assert (
                address.account_id == acc2.id
            ), "expected all addresses to belong to account 2"
