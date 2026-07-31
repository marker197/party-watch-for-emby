"""Rating Bias Detector Service — Idea #3 from Novel Ideas.

Analyzes Simkl rating history to:
1. Identify rating patterns and biases
2. Suggest systematically underrated content
3. Find "against your taste" items for exploration
4. Predict rating blind spots
5. Create challenge collections
"""

from .service import RatingBiasDetectorService

__all__ = ["RatingBiasDetectorService"]
