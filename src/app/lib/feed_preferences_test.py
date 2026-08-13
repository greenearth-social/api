from ..documents import FeedPreferencesDocument, SourceWeightsDocument, UserDocument
from .feed_preferences import DEFAULT_SOURCE_WEIGHTS, resolve_feed_preferences


def test_resolver_prefers_atomic_source_weights_over_legacy_values():
    # Three-source documents remain valid and resolve Network Likes to zero.
    weights = SourceWeightsDocument(
        following=0.5,
        authors_topics=0.2,
        popular=0.3,
    )
    user = UserDocument(
        user_did="did:plc:test",
        social_radius=0,
        feed_preferences={
            "your-feed": FeedPreferencesDocument(
                source_weights=weights,
                social_radius=4,
            )
        },
    )

    resolved = resolve_feed_preferences(user, "your-feed")

    assert resolved.source_weights == weights
    assert resolved.social_radius is None


def test_resolver_lazily_translates_feed_scoped_social_radius():
    user = UserDocument(
        user_did="did:plc:test",
        social_radius=0,
        feed_preferences={
            "your-feed": FeedPreferencesDocument(social_radius=4),
        },
    )

    resolved = resolve_feed_preferences(user, "your-feed")

    assert resolved.source_weights == SourceWeightsDocument(
        following=0.1,
        network_likes=0.1,
        authors_topics=0.4,
        popular=0.4,
    )


def test_resolver_translates_flat_legacy_social_radius():
    user = UserDocument(user_did="did:plc:test", social_radius=1)

    resolved = resolve_feed_preferences(user, "your-feed")

    assert resolved.source_weights == SourceWeightsDocument(
        following=0.7,
        network_likes=0.1,
        authors_topics=0.1,
        popular=0.1,
    )


def test_resolver_uses_source_weight_defaults_without_a_user():
    resolved = resolve_feed_preferences(None, "your-feed")

    assert resolved.source_weights == DEFAULT_SOURCE_WEIGHTS
