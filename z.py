# # from sqlalchemy import inspect
# # from scbl_utils.db_models import bases, data, definitions
# # print()
# # print(inspect(data.Person).base_mapper.class_)


# from pathlib import Path

# for current, dirnames, filenames in Path.cwd().walk():
#     for fname in filenames:
#         try:
#             if 'OBJECT_SEP_CHAR' in (current /fname).read_text():
#                 print(str(current / fname))
#         except:
#             pass

import email_validator

email_validator.validate_email('ssdf@jax.org', check_deliverability=True)
