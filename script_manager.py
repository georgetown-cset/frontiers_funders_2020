# # screen
# git fetch --all
# git reset --hard
# git pull  'https://github.com/georgetown-cset/science_map'
# git reset --hard master
# python script_manager.py
#https://www.tecmint.com/fix-git-user-credentials-for-https/

#ir177$ source /Users/ir177/.virtualenvs/Documents/GitHub/science_map/bin/activate


from helpers.functions import start_debug, add_acc_message
from helpers.BQ_dataset_update_functions import run_bq_queries




if __name__ == "__main__":
    # start debuging file:
    start_debug()
    add_acc_message("Started running frontiers code.")
    # create data directories if nedeed:
    add_acc_message("Started running frontiers code.")
    add_acc_message("Updating BQ tables.")
    for year in [2014, 2016, 2020]:
        run_bq_queries(year)


