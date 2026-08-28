# Pre-token-generation do Cognito: injeta o claim 'role' no ID token a partir
# do grupo do usuario. O webapp argus decide o que mostrar por esse claim
# (decodeAccessClaims exige 'role' no payload).
ORDEM = ("ADMIN", "OPERATOR", "VIEWER")


def handler(event, context):
    grupos = (
        event.get("request", {})
        .get("groupConfiguration", {})
        .get("groupsToOverride")
        or []
    )
    # Usuario em mais de um grupo fica com o mais forte.
    papel = next((g for g in ORDEM if g in grupos), "VIEWER")
    event["response"] = {
        "claimsOverrideDetails": {"claimsToAddOrOverride": {"role": papel}}
    }
    return event
