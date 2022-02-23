from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

# oauth_client = BackendApplicationClient(client_id="9326820e9ba14d9b822f51010028cf00")
# oauth_session = OAuth2Session(client=oauth_client)
oauth_session = OAuth2Session(client_id="9326820e9ba14d9b822f51010028cf00")
token = oauth_session.refresh_token(
    token_url="https://accounts.pod.ir/oauth2/token"
    ,refresh_token="2ba09737435f4a0eb0cbfce380d90573"
    ,body="grant_type=refresh_token&client_id=9326820e9ba14d9b822f51010028cf00&refresh_token=2ba09737435f4a0eb0cbfce380d90573&code_verifier=1KbYzrQBANWOk2tQIezpWHzEPty0vir1Z_DrmYjYVr0"
    )
# ,refresh_token="02673a32d530494b985cf25aac82508b"
print(token)