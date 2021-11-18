module integration.it;

import unit_threaded;

int main(string[] args)
{
    return args.runTests!(
        "integration.curl",
        "integration.vibe",
        "integration.ion",
        "integration.api",
    );
}
