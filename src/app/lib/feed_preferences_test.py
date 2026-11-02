import pytest
from pydantic import ValidationError

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
    assert resolved.author_penalty == 0.7
    assert resolved.topic_penalty == 0.7


def test_resolver_keeps_penalties_independent_and_feed_scoped():
    user = UserDocument(
        user_did="did:plc:test",
        feed_preferences={
            "your-feed": FeedPreferencesDocument(author_penalty=0.2, topic_penalty=0.8),
            "best-of-friends": FeedPreferencesDocument(author_penalty=0.9, topic_penalty=0.1),
        },
    )

    assert resolve_feed_preferences(user, "your-feed").author_penalty == 0.2
    assert resolve_feed_preferences(user, "your-feed").topic_penalty == 0.8
    assert resolve_feed_preferences(user, "best-of-friends").author_penalty == 0.9
    assert resolve_feed_preferences(user, "best-of-friends").topic_penalty == 0.1


@pytest.mark.parametrize("field", ["author_penalty", "topic_penalty"])
@pytest.mark.parametrize("value", [0.0, 1.0])
def test_feed_preferences_accept_penalty_boundaries(field, value):
    assert getattr(FeedPreferencesDocument(**{field: value}), field) == value


@pytest.mark.parametrize("field", ["author_penalty", "topic_penalty"])
@pytest.mark.parametrize("value", [-0.01, 1.01])
def test_feed_preferences_reject_penalties_outside_boundaries(field, value):
    with pytest.raises(ValidationError):
        FeedPreferencesDocument(**{field: value})


def test_resolver_prefers_feed_scoped_politics_over_legacy_value():
    user = UserDocument(
        user_did="did:plc:test",
        politics=0.25,
        feed_preferences={
            "your-feed": FeedPreferencesDocument(politics=0.0),
        },
    )

    resolved = resolve_feed_preferences(user, "your-feed")

    assert resolved.politics == 0.0


@pytest.mark.parametrize(
    "feed_name", ["your-feed", "best-of-friends", "cutoff-preview", "unranked-your-feed"]
)
@pytest.mark.parametrize("user", [None, UserDocument(user_did="did:plc:test")])
def test_resolver_uses_default_politics_for_new_users(feed_name, user):
    assert resolve_feed_preferences(user, feed_name).politics == 0.5


@pytest.mark.parametrize("politics", [0.0, 2.0])
def test_feed_preferences_document_accepts_politics_boundaries(politics):
    assert FeedPreferencesDocument(politics=politics).politics == politics


@pytest.mark.parametrize("politics", [-0.01, 2.01])
def test_feed_preferences_document_rejects_politics_outside_boundaries(politics):
    with pytest.raises(ValidationError):
        FeedPreferencesDocument(politics=politics)
