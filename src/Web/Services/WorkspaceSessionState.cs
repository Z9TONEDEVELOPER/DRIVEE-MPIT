using NexusDataSpace.Core.Models;
using NexusDataSpace.Web.Models;

namespace NexusDataSpace.Web.Services;

public class WorkspaceSessionState
{
    public string Query { get; set; } = "";
    public bool Busy { get; set; }
    public string ThinkingLabel { get; set; } = "\u0414\u0443\u043c\u0430\u044e\u2026";
    public List<WorkspaceChatMessage> Messages { get; } = new();

    public event Action? Changed;

    public void ClearChat()
    {
        Messages.Clear();
        Query = "";
        Busy = false;
        ThinkingLabel = "\u0414\u0443\u043c\u0430\u044e\u2026";
        NotifyChanged();
    }

    public void NotifyChanged() => Changed?.Invoke();
}
