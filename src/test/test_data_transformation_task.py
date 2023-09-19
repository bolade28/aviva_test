import json
import pytest
import pandas as pd
from src.aviva import data_transformation_task as task

test_data = """
[{"abstract": {"_value": "MPs should attend all debates, not merely turn up and vote or strike pairing deals. With other commitments, a five day Commons is not workable for MPs: I suggest three full days (9am to 6pm minimum), with one or two days for Committees, leaving at least one day for constituency work."}, "label": {"_value": "Reform the Commons: Three days full time with compulsory attendance for all MPs."}, "numberOfSignatures": 27}, {"abstract": {"_value": "When you change your car you pay road tax for the whole month on your old car and on your new car, effectively paying for two cars when you own only one. Before recent changes to road tax discs and on line payments the tax was carried over to the new owner!"}, "label": {"_value": "Instruct the DVLA to charge road tax during the month of sale on a daily basis."}, "numberOfSignatures": 223}, {"abstract": {"_value": "CofE attendance is 765,000, representing 1.4% of England's population. The House of Lords has 26 CofE Bishops who participate in debates and vote in divisions, which involve decisions affecting the entire UK. This is wholly disproportionate to the size of the CofE's attendance and thus influence."}, "label": {"_value": "Disestablish the Church of England and establish Britain as a secular state"}, "numberOfSignatures": 176}, {"abstract": {"_value": "Once the Leeming to Barton stretch of the A1(M) is completed, the route from Aberford to Birtley, near Gateshead, should be renamed as the M1. This would boost economic growth in the North East and North Yorkshire and would be easier for foreign drivers as their is no longer a sudden number change."}, "label": {"_value": "Once completed, Renumber the A1(M) North of Aberford as the M1."}, "numberOfSignatures": 15}]
"""


@pytest.fixture
def df_gen() -> pd.DataFrame:
    data = json.loads(test_data)
    df = pd.DataFrame(data)
    return df


def test_for_number_of_columns(df_gen) -> None:
    df_out = task.perform_transform(df_gen)
    assert df_out.shape == (4, 21)


def test_for_uniqueness_pk_field(df_gen) -> None:
    df_out = task.perform_transform(df_gen)
    df2 = df_out[df_out.duplicated('petition_id')]
    assert len(df2) == 0


def test_for_row_count(df_gen):
    df_out = task.perform_transform(df_gen)
    assert len(df_out) == 4


def test_file_not_found_error():
    with pytest.raises(FileNotFoundError):
        task.gen_init_df_from_json('test1.json')
