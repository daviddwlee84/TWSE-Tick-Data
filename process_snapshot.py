import fire
from data_v2 import SnapshotParser

def parse_snapshot_and_save(filepath: str, output_dir: str):
    parser = SnapshotParser()
    df = parser.load_dsp_file(filepath)
    parser.save_by_securities(df, output_dir)


if __name__ == '__main__':
    fire.Fire(parse_snapshot_and_save)