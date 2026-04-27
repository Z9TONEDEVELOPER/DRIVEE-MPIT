using NexusDataSpace.Core.Models;

namespace NexusDataSpace.Core.Services;

public sealed class TenantContext
{
    private readonly AsyncLocal<int?> _companyId = new();

    public int CompanyId => _companyId.Value ?? CompanyDefaults.DefaultCompanyId;

    public IDisposable Use(int companyId)
    {
        var previous = _companyId.Value;
        _companyId.Value = companyId <= 0 ? CompanyDefaults.DefaultCompanyId : companyId;
        return new Scope(this, previous);
    }

    private sealed class Scope : IDisposable
    {
        private readonly TenantContext _context;
        private readonly int? _previous;

        public Scope(TenantContext context, int? previous)
        {
            _context = context;
            _previous = previous;
        }

        public void Dispose() => _context._companyId.Value = _previous;
    }
}
