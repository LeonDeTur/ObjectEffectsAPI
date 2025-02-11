from .dto.effects_dto import EffectsDTO


class EffectsService:
    """
    Class for handling services calculation
    """

    @staticmethod
    async def calculate_effects(params: EffectsDTO) -> dict:
        """
        Calculate provision effects by project data and target scenario
        Args:
            params (EffectsDTO): Project data
        Returns:
             dict: Provision effects
        """

        pass
