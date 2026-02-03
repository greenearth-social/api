
from dotenv import load_dotenv

# Load environment variables from .env as early as possible so modules
# which read os.environ at import-time get the configured values.
# (confusingly, pipenv loads .env, so in some cases this is redundant)
load_dotenv()
